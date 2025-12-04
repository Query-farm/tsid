#define DUCKDB_EXTENSION_MAIN
#include "tsid_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "uutid.hpp"

namespace duckdb {

// Generate new TSID
static void TsidScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto result_data = FlatVector::GetData<string_t>(result);
    auto &validity = FlatVector::Validity(result);

    for (idx_t i = 0; i < args.size(); i++) {
        auto id = UUTID::new_id();
        result_data[i] = StringVector::AddString(result, id.to_string());
        validity.Set(i, true);
    }
}

// Extract timestamp from TSID
static void TsidToTimestampScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, timestamp_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            try {
                auto id = UUTID::from_string(input.GetString());
                auto tp = id.time();
                auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
                    tp.time_since_epoch()).count();
                return timestamp_t(micros);
            } catch (const std::exception &e) {
                throw ConversionException("Invalid TSID format: %s", input.GetString());
            }
        });
}

static void LoadInternal(ExtensionLoader &loader) {
    // Register tsid() function
    ScalarFunction tsid_fun({}, LogicalType::VARCHAR, TsidScalarFun);
    tsid_fun.stability = FunctionStability::VOLATILE;

    ScalarFunctionSet tsid_set("tsid");
    tsid_set.AddFunction(tsid_fun);

    CreateScalarFunctionInfo tsid_info(tsid_set);
    FunctionDescription tsid_desc;
    tsid_desc.description = "Generates a new Time-Sorted Unique Identifier (TSID). "
                            "TSIDs are chronologically sortable 128-bit unique identifiers "
                            "that embed a timestamp, making them ideal for distributed systems "
                            "and time-series data.";
    tsid_desc.examples = {"tsid()"};
    tsid_desc.categories = {"uuid"};
    tsid_info.descriptions.push_back(std::move(tsid_desc));

    loader.RegisterFunction(std::move(tsid_info));

    // Register tsid_to_timestamp() function
    ScalarFunction tsid_to_ts_fun("tsid_to_timestamp", {LogicalType::VARCHAR}, LogicalType::TIMESTAMP,
                                   TsidToTimestampScalarFun);

    ScalarFunctionSet tsid_to_ts_set("tsid_to_timestamp");
    tsid_to_ts_set.AddFunction(tsid_to_ts_fun);

    CreateScalarFunctionInfo tsid_to_ts_info(tsid_to_ts_set);
    FunctionDescription tsid_to_ts_desc;
    tsid_to_ts_desc.parameter_names = {"tsid"};
    tsid_to_ts_desc.parameter_types = {LogicalType::VARCHAR};
    tsid_to_ts_desc.description = "Extracts the embedded timestamp from a TSID. "
                                   "Returns the timestamp that was recorded when the TSID was generated.";
    tsid_to_ts_desc.examples = {"tsid_to_timestamp('0193b9c8d23d7192bc1cc82b43e6e8f3')"};
    tsid_to_ts_desc.categories = {"uuid"};
    tsid_to_ts_info.descriptions.push_back(std::move(tsid_to_ts_desc));

    loader.RegisterFunction(std::move(tsid_to_ts_info));
}

void TsidExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}

std::string TsidExtension::Name() {
    return "tsid";
}

std::string TsidExtension::Version() const {
#ifdef EXT_VERSION_TSID
    return EXT_VERSION_TSID;
#else
    return "";
#endif
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
