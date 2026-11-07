#ifndef UUTID_HPP
#define UUTID_HPP

// UUTID: a 128-bit, time-sorted identifier laid out like a UUID v4.
//
//   bytes 0-3   seconds since the Unix epoch (big-endian, unsigned 32-bit: 1970..2106)
//   bytes 4-7   nanoseconds (30 bits, 4 ns resolution) with the version nibble (4) in byte 6
//   byte  8     RFC 4122 variant bits (10xxxxxx)
//   bytes 8-15  random
//
// The class holds no global state: callers pass the random engine to use, so every thread
// (or every DuckDB expression state) can own its own engine.

#include <array>
#include <cstddef>
#include <cstdint>

class UUTID {
public:
	static constexpr size_t SIZE = 16;
	static constexpr uint8_t VERSION = 4;
	//! Largest representable seconds value (bytes 0-3 are an unsigned 32-bit integer)
	static constexpr int64_t MAX_SECONDS = 0xFFFFFFFFLL;
	using ByteArray = std::array<uint8_t, SIZE>;

	UUTID() : data_() {
	}

	//! Build an id for `seconds` (0..MAX_SECONDS) and `nanos` (0..999999999) since the epoch.
	//! `random_tail` supplies bytes 8-15 (the top two bits of byte 8 are replaced by the variant).
	static UUTID FromParts(uint32_t seconds, uint32_t nanos, uint64_t random_tail) {
		UUTID id;
		uint32_t nsec = nanos << 2; // nanos < 2^30, so this cannot overflow
		uint16_t ns1 = static_cast<uint16_t>((nsec >> 16) & 0xffff);
		uint16_t ns2 = static_cast<uint16_t>(nsec & 0xffff);
		ns2 = static_cast<uint16_t>((ns2 >> 4) & 0x0fff);
		ns2 = static_cast<uint16_t>(ns2 | (VERSION << 12));

		id.data_[0] = static_cast<uint8_t>((seconds >> 24) & 0xFF);
		id.data_[1] = static_cast<uint8_t>((seconds >> 16) & 0xFF);
		id.data_[2] = static_cast<uint8_t>((seconds >> 8) & 0xFF);
		id.data_[3] = static_cast<uint8_t>(seconds & 0xFF);
		id.data_[4] = static_cast<uint8_t>((ns1 >> 8) & 0xFF);
		id.data_[5] = static_cast<uint8_t>(ns1 & 0xFF);
		id.data_[6] = static_cast<uint8_t>((ns2 >> 8) & 0xFF);
		id.data_[7] = static_cast<uint8_t>(ns2 & 0xFF);
		for (size_t i = 0; i < 8; i++) {
			id.data_[8 + i] = static_cast<uint8_t>((random_tail >> (56 - 8 * i)) & 0xFF);
		}
		id.data_[8] = static_cast<uint8_t>((id.data_[8] & 0x3f) | 0x80);
		return id;
	}

	static UUTID FromBytes(const uint8_t *bytes) {
		UUTID id;
		for (size_t i = 0; i < SIZE; i++) {
			id.data_[i] = bytes[i];
		}
		return id;
	}

	//! Strict parser. Accepts exactly 32 hex digits, or 36 characters in the canonical
	//! 8-4-4-4-12 layout with dashes at positions 8, 13, 18 and 23. Upper and lower case
	//! hex digits are accepted. Returns false (never throws, never reads out of bounds) otherwise.
	static bool TryParse(const char *str, size_t len, UUTID &out) {
		if (len != 32 && len != 36) {
			return false;
		}
		const bool dashed = len == 36;
		size_t byte_idx = 0;
		int high = -1;
		for (size_t i = 0; i < len; i++) {
			const char c = str[i];
			if (dashed && (i == 8 || i == 13 || i == 18 || i == 23)) {
				if (c != '-') {
					return false;
				}
				continue;
			}
			const int v = HexValue(c);
			if (v < 0) {
				return false;
			}
			if (high < 0) {
				high = v;
			} else {
				if (byte_idx >= SIZE) {
					return false;
				}
				out.data_[byte_idx++] = static_cast<uint8_t>((high << 4) | v);
				high = -1;
			}
		}
		return byte_idx == SIZE && high < 0;
	}

	//! 32 lowercase hex digits
	void ToHex(char *out) const {
		static const char digits[] = "0123456789abcdef";
		for (size_t i = 0; i < SIZE; i++) {
			out[2 * i] = digits[data_[i] >> 4];
			out[2 * i + 1] = digits[data_[i] & 0x0F];
		}
	}

	uint32_t Seconds() const {
		return (static_cast<uint32_t>(data_[0]) << 24) | (static_cast<uint32_t>(data_[1]) << 16) |
		       (static_cast<uint32_t>(data_[2]) << 8) | static_cast<uint32_t>(data_[3]);
	}

	uint32_t Nanos() const {
		uint16_t ns1 = static_cast<uint16_t>((static_cast<uint16_t>(data_[4]) << 8) | data_[5]);
		uint16_t ns2 = static_cast<uint16_t>((static_cast<uint16_t>(data_[6]) << 8) | data_[7]);
		ns2 = static_cast<uint16_t>((ns2 & 0x0fff) << 4);
		uint32_t nsec = (static_cast<uint32_t>(ns1) << 16) | ns2;
		return nsec >> 2;
	}

	//! Microseconds since the epoch (truncated)
	int64_t EpochMicros() const {
		return static_cast<int64_t>(Seconds()) * 1000000LL + static_cast<int64_t>(Nanos() / 1000);
	}

	const ByteArray &Bytes() const {
		return data_;
	}

private:
	ByteArray data_;

	static int HexValue(char c) {
		if (c >= '0' && c <= '9') {
			return c - '0';
		}
		if (c >= 'a' && c <= 'f') {
			return c - 'a' + 10;
		}
		if (c >= 'A' && c <= 'F') {
			return c - 'A' + 10;
		}
		return -1;
	}
};

#endif // UUTID_HPP
