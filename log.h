#pragma once

#include <bitset>

#include <iostream>

#include <iomanip>

#include <cstddef>

#include <cstdint>

#include <optional>

#include <stdlib.h>

#include <string>

#include <tuple>

#include <vector>

using namespace std;
















#define _chars(w) string(#w)


constexpr int transaction_number_size = sizeof(uint64_t);

constexpr int key_size = sizeof(int64_t);

constexpr int64_t key_complete = 0;


struct log_entry {

	enum log_entry_type :int8_t {

		normal = 0, erase = 1, complete = 2,

	};

	log_entry(uint64_t, int64_t, vector<char>, log_entry_type);

	static log_entry create_log_entry(uint64_t, int64_t, vector<char>, log_entry_type);

	static log_entry create_log_entry_normal(uint64_t, int64_t, vector<char>);

	static log_entry create_log_entry_erase(uint64_t, int64_t);

	static log_entry create_log_entry_complete(uint64_t);

	inline auto relate_transaction()const;

	inline auto value_size()const;

	void print()const;

	uint64_t _transaction_number;

	int64_t _key;

	vector<char> _value;

	log_entry_type _type;

};


auto log_entry::relate_transaction()const {

	return _transaction_number != 0;

}

auto log_entry::value_size()const {

	return static_cast<uint32_t>(_value.size());

}


struct log_entry_position {

	log_entry_position();

	log_entry_position(uint32_t, int64_t);

	bool operator==(const log_entry_position&)const = default;

	void print()const;

	uint32_t _file_i;

	int64_t _offset;

};

constexpr int log_entry_position_store_size = sizeof(log_entry_position::_file_i) + sizeof(log_entry_position::_offset);


struct log_entry_tag {
	
	log_entry_tag(uint32_t, uint32_t, log_entry::log_entry_type);

	void print()const;

	uint32_t _cyclic;

	uint32_t _v_size;

	log_entry::log_entry_type _type;

};

constexpr int log_entry_tag_store_size = sizeof(log_entry_tag::_cyclic) + sizeof(log_entry_tag::_v_size) + sizeof(log_entry_tag::_type);


struct hint {

	hint(int64_t, log_entry_position);

	hint(const log_entry&);

	void print()const;

	int64_t _key;

	log_entry_position _position;

};


constexpr int hint_store_size = sizeof(hint::_key) + sizeof(hint::_position);


vector<char> serialize_i(uint32_t);


vector<char> serialize_hint(const hint&);


vector<char> serialize_key(int64_t);


vector<char> serialize_log_entry(const log_entry&);


vector<char> serialize_log_entry_position(const log_entry_position&);


uint32_t e_serialize_i(const vector<char>&);


log_entry_position e_serialize_log_entry_position(const vector<char>&);


log_entry_tag e_serialize_log_entry_tag(const vector<char>&);


tuple<uint64_t, int64_t, vector<char>> e_serialize_n_k_v(const vector<char>&, uint32_t);


