#define DUCKDB_EXTENSION_MAIN
#include "tsid_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "uutid.hpp"
#include "query_farm_telemetry.hpp"

#include <chrono>
#include <random>
#include <thread>

namespace duckdb {

//! Single source of truth for the extension version (Version() and telemetry).
static constexpr const char *TSID_EXTENSION_VERSION = "2026110701";

//------------------------------------------------------------------------------
// Random state: one engine per expression state (i.e. per executing thread),
// seeded from std::random_device mixed with the clock and thread id, so no
// engine is ever shared between threads and different processes do not collide.
//------------------------------------------------------------------------------
struct TsidLocalState : public FunctionLocalState {
	std::mt19937_64 engine;

	TsidLocalState() {
		std::random_device rd;
		const auto now = static_cast<uint64_t>(std::chrono::high_resolution_clock::now().time_since_epoch().count());
		const auto tid = static_cast<uint64_t>(std::hash<std::thread::id>()(std::this_thread::get_id()));
		const auto addr = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(this));
		std::seed_seq seq {rd(),
		                   rd(),
		                   rd(),
		                   rd(),
		                   static_cast<uint32_t>(now),
		                   static_cast<uint32_t>(now >> 32),
		                   static_cast<uint32_t>(tid),
		                   static_cast<uint32_t>(tid >> 32),
		                   static_cast<uint32_t>(addr),
		                   static_cast<uint32_t>(addr >> 32)};
		engine.seed(seq);
	}
};

static unique_ptr<FunctionLocalState> TsidInitLocalState(ExpressionState &, const BoundFunctionExpression &,
                                                         FunctionData *) {
	return make_uniq<TsidLocalState>();
}

static std::mt19937_64 &GetEngine(ExpressionState &state) {
	return ExecuteFunctionState::GetFunctionState(state)->Cast<TsidLocalState>().engine;
}

static UUTID NewIdNow(std::mt19937_64 &engine) {
	const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
	                    std::chrono::system_clock::now().time_since_epoch())
	                    .count();
	const int64_t seconds = ns / 1000000000LL;
	const int64_t nanos = ns % 1000000000LL;
	return UUTID::FromParts(static_cast<uint32_t>(seconds), static_cast<uint32_t>(nanos), engine());
}

static UUTID NewIdAt(timestamp_t ts, std::mt19937_64 &engine) {
	if (ts == timestamp_t::infinity() || ts == timestamp_t::ninfinity()) {
		throw OutOfRangeException("tsid: cannot encode an infinite timestamp");
	}
	const int64_t micros = ts.value;
	if (micros < 0 || micros / 1000000LL > UUTID::MAX_SECONDS) {
		throw OutOfRangeException("tsid: timestamp %s is outside the encodable range "
		                          "1970-01-01 00:00:00 .. 2106-02-07 06:28:15",
		                          Timestamp::ToString(ts));
	}
	const int64_t seconds = micros / 1000000LL;
	const int64_t nanos = (micros % 1000000LL) * 1000LL;
	return UUTID::FromParts(static_cast<uint32_t>(seconds), static_cast<uint32_t>(nanos), engine());
}

static string_t IdToVarchar(const UUTID &id, Vector &result) {
	char buf[32];
	id.ToHex(buf);
	return StringVector::AddString(result, buf, 32);
}

// DuckDB stores a UUID as a hugeint whose most significant bit is flipped so that
// signed comparison matches the byte-wise (string) order.
static hugeint_t IdToUuid(const UUTID &id) {
	const auto &b = id.Bytes();
	uint64_t upper = 0;
	uint64_t lower = 0;
	for (idx_t i = 0; i < 8; i++) {
		upper = (upper << 8) | b[i];
		lower = (lower << 8) | b[8 + i];
	}
	hugeint_t result;
	result.lower = lower;
	result.upper = static_cast<int64_t>(upper ^ (uint64_t(1) << 63));
	return result;
}

static UUTID UuidToId(hugeint_t input) {
	uint8_t bytes[16];
	const uint64_t upper = static_cast<uint64_t>(input.upper) ^ (uint64_t(1) << 63);
	const uint64_t lower = input.lower;
	for (idx_t i = 0; i < 8; i++) {
		bytes[i] = static_cast<uint8_t>((upper >> (56 - 8 * i)) & 0xFF);
		bytes[8 + i] = static_cast<uint8_t>((lower >> (56 - 8 * i)) & 0xFF);
	}
	return UUTID::FromBytes(bytes);
}

//------------------------------------------------------------------------------
// Generators
//------------------------------------------------------------------------------
static void TsidScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &engine = GetEngine(state);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i] = IdToVarchar(NewIdNow(engine), result);
	}
}

static void TsidUuidScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &engine = GetEngine(state);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetDataMutable<hugeint_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i] = IdToUuid(NewIdNow(engine));
	}
}

// The timestamp argument is often constant (e.g. tsid(TIMESTAMP 2024-01-01) over a table):
// always produce a FLAT result with a fresh id per row, never a single constant value.
template <class RESULT_TYPE, class OP>
static void ExecutePerRow(DataChunk &args, Vector &result, OP op) {
	const idx_t count = args.size();
	UnifiedVectorFormat input;
	args.data[0].ToUnifiedFormat(input);
	auto input_data = input.GetData<timestamp_t>();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetDataMutable<RESULT_TYPE>(result);
	auto &result_validity = FlatVector::ValidityMutable(result);
	for (idx_t i = 0; i < count; i++) {
		const auto idx = input.sel->get_index(i);
		if (!input.validity.RowIsValid(idx)) {
			result_validity.SetInvalid(i);
			continue;
		}
		result_data[i] = op(input_data[idx]);
	}
}

static void TsidAtScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &engine = GetEngine(state);
	ExecutePerRow<string_t>(args, result, [&](timestamp_t ts) { return IdToVarchar(NewIdAt(ts, engine), result); });
}

static void TsidUuidAtScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &engine = GetEngine(state);
	ExecutePerRow<hugeint_t>(args, result, [&](timestamp_t ts) { return IdToUuid(NewIdAt(ts, engine)); });
}

//------------------------------------------------------------------------------
// Decoders
//------------------------------------------------------------------------------
static void TsidToTimestampScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, timestamp_t>(args.data[0], result, args.size(), [&](string_t input) {
		UUTID id;
		if (!UUTID::TryParse(input.GetData(), input.GetSize(), id)) {
			throw ConversionException("Invalid TSID format: '%s' (expected 32 hex digits, optionally "
			                          "dashed as 8-4-4-4-12)",
			                          input.GetString());
		}
		return timestamp_t(id.EpochMicros());
	});
}

static void TsidUuidToTimestampScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<hugeint_t, timestamp_t>(args.data[0], result, args.size(), [&](hugeint_t input) {
		return timestamp_t(UuidToId(input).EpochMicros());
	});
}

//------------------------------------------------------------------------------
// Registration
//------------------------------------------------------------------------------
static FunctionDescription MakeDescription(vector<LogicalType> parameter_types, const string &description,
                                           vector<string> examples) {
	FunctionDescription desc;
	desc.parameter_types = std::move(parameter_types);
	desc.description = description;
	desc.examples = std::move(examples);
	desc.categories = {"uuid"};
	return desc;
}

static void RegisterSet(ExtensionLoader &loader, ScalarFunctionSet set, vector<FunctionDescription> descriptions) {
	CreateScalarFunctionInfo info(std::move(set));
	info.descriptions = std::move(descriptions);
	loader.RegisterFunction(std::move(info));
}

static FunctionParameter Positional(const char *name, const LogicalType &type) {
	return FunctionParameter(name, type, {}, FunctionParameterKind::POSITIONAL_ONLY);
}

static void LoadInternal(ExtensionLoader &loader) {
	// tsid() / tsid(ts) -> VARCHAR
	{
		ScalarFunctionSet set("tsid");
		ScalarFunction now_fun({}, LogicalType::VARCHAR, TsidScalarFun);
		now_fun.SetVolatile();
		now_fun.SetInitStateCallback(TsidInitLocalState);
		set.AddFunction(now_fun);

		ScalarFunction at_fun("tsid", {Positional("ts", LogicalType::TIMESTAMP)}, LogicalType::VARCHAR,
		                      TsidAtScalarFun);
		at_fun.SetVolatile();
		at_fun.SetFallible();
		at_fun.SetInitStateCallback(TsidInitLocalState);
		set.AddFunction(at_fun);

		RegisterSet(loader, std::move(set),
		            {MakeDescription({}, "Generates a new time-sorted unique identifier (TSID) as 32 lowercase hex "
		                             "digits. The first 64 bits encode the current time (seconds and "
		                             "nanoseconds since the Unix epoch), so TSIDs sort chronologically; the "
		                             "remaining bits are random. The bit layout is UUID v4 compatible.",
		                             {"tsid()", "SELECT tsid() AS id FROM range(3) ORDER BY id"}),
		             MakeDescription({LogicalType::TIMESTAMP}, "Generates a TSID whose embedded time is the given timestamp (1970-01-01 "
		                             "to 2106-02-07); the remaining bits are random. Useful for backfilling "
		                             "or for building range bounds.",
		                             {"tsid(TIMESTAMP '2024-06-01 12:00:00')",
		                              "tsid_to_timestamp(tsid(TIMESTAMP '2024-06-01 12:00:00.123456'))"})});
	}

	// tsid_uuid() / tsid_uuid(ts) -> UUID
	{
		ScalarFunctionSet set("tsid_uuid");
		ScalarFunction now_fun({}, LogicalType::UUID, TsidUuidScalarFun);
		now_fun.SetVolatile();
		now_fun.SetInitStateCallback(TsidInitLocalState);
		set.AddFunction(now_fun);

		ScalarFunction at_fun("tsid_uuid", {Positional("ts", LogicalType::TIMESTAMP)}, LogicalType::UUID,
		                      TsidUuidAtScalarFun);
		at_fun.SetVolatile();
		at_fun.SetFallible();
		at_fun.SetInitStateCallback(TsidInitLocalState);
		set.AddFunction(at_fun);

		RegisterSet(loader, std::move(set),
		            {MakeDescription({}, "Generates a new TSID as a native UUID value (16 bytes instead of a "
		                             "32-character string). Same layout and ordering as tsid().",
		                             {"tsid_uuid()", "SELECT tsid_uuid() AS id FROM range(3) ORDER BY id"}),
		             MakeDescription({LogicalType::TIMESTAMP}, "Generates a TSID as a native UUID whose embedded time is the given "
		                             "timestamp (1970-01-01 to 2106-02-07).",
		                             {"tsid_uuid(TIMESTAMP '2024-06-01 12:00:00')"})});
	}

	// tsid_to_timestamp(VARCHAR | UUID) -> TIMESTAMP
	{
		ScalarFunctionSet set("tsid_to_timestamp");
		ScalarFunction str_fun("tsid_to_timestamp", {Positional("tsid", LogicalType::VARCHAR)},
		                       LogicalType::TIMESTAMP, TsidToTimestampScalarFun);
		str_fun.SetFallible();
		set.AddFunction(str_fun);

		ScalarFunction uuid_fun("tsid_to_timestamp", {Positional("tsid", LogicalType::UUID)}, LogicalType::TIMESTAMP,
		                        TsidUuidToTimestampScalarFun);
		set.AddFunction(uuid_fun);

		RegisterSet(loader, std::move(set),
		            {MakeDescription({LogicalType::VARCHAR}, "Extracts the embedded timestamp (microsecond precision) from a TSID "
		                             "string. Accepts 32 hex digits or the dashed 8-4-4-4-12 form; anything "
		                             "else raises a conversion error.",
		                             {"tsid_to_timestamp('65d4a2b6000000004000800000000000')",
		                              "tsid_to_timestamp('65d4a2b6-0000-0000-4000-800000000000')"}),
		             MakeDescription({LogicalType::UUID}, "Extracts the embedded timestamp (microsecond precision) from a TSID "
		                             "stored as a native UUID.",
		                             {"tsid_to_timestamp(tsid_uuid())",
		                              "tsid_to_timestamp('65d4a2b6-0000-0000-4000-800000000000'::UUID)"})});
	}

	QueryFarmSendTelemetry(loader, "tsid", TSID_EXTENSION_VERSION);
}

void TsidExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string TsidExtension::Name() {
	return "tsid";
}

std::string TsidExtension::Version() const {
	return TSID_EXTENSION_VERSION;
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(tsid, loader) {
	duckdb::LoadInternal(loader);
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
