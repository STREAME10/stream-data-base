#include "log.h"






































log_entry log_entry::create_log_entry(uint64_t transaction_number, int64_t key, vector<char> value, log_entry_type type) {

	return log_entry(transaction_number, key, move(value), type);

}


log_entry log_entry::create_log_entry_normal(uint64_t transaction_number, int64_t key, vector<char> value) {
	
	return log_entry(transaction_number, key, move(value), log_entry::normal);

}


log_entry log_entry::create_log_entry_erase(uint64_t transaction_number, int64_t key) {

	return log_entry(transaction_number, key, vector<char>(), log_entry::erase);

}


log_entry log_entry::create_log_entry_complete(uint64_t transaction_number) {

	return log_entry(transaction_number, key_complete, vector<char>(), log_entry::complete);

}


log_entry::log_entry(uint64_t transaction_number, int64_t key, vector<char> value, log_entry_type type) :
	_transaction_number(transaction_number), _key(key), _value(move(value)), _type(type) {

}


log_entry_position::log_entry_position() :
	log_entry_position(0, 0) {

}


log_entry_position::log_entry_position(uint32_t file_i, int64_t offset) :
	_file_i(file_i), _offset(offset) {

}


log_entry_tag::log_entry_tag(uint32_t cyclic, uint32_t v_size, log_entry::log_entry_type type) :
	_cyclic(cyclic), _v_size(v_size), _type(type) {

}


hint::hint(int64_t key, log_entry_position position) :
	_key(key), _position(position) {

}


hint::hint(const log_entry& entry) :
	_key(entry._key), _position(e_serialize_log_entry_position(entry._value)) {

}


vector<char> serialize_i(uint32_t i) {

	vector<char> v(sizeof(i));

	memmove(&v[0], &i, sizeof(i));

	return move(v);

}


uint32_t e_serialize_i(const vector<char>& v) {

	return *reinterpret_cast<const uint32_t*>(&v[0]);

}


vector<char> serialize_hint(const hint & i) {

	return serialize_log_entry(log_entry(0, i._key, serialize_log_entry_position(i._position), log_entry::normal));

}


vector<char> serialize_key(int64_t key) {

	vector<char> v(key_size);

	memmove(&v[0], &key, key_size);

	return move(v);

}


vector<char> serialize_log_entry(const log_entry& entry) {

	auto value_size = entry.value_size();

	vector<char> v(log_entry_tag_store_size + transaction_number_size + key_size + value_size);

	auto p = &v[0];

	memmove((p += sizeof(log_entry_tag::_cyclic)), &entry._type, sizeof(log_entry_tag::_type));

	memmove((p += sizeof(log_entry_tag::_type)), &value_size, sizeof(log_entry_tag::_v_size));

	memmove((p += sizeof(log_entry_tag::_v_size)), &entry._transaction_number, transaction_number_size);

	memmove((p += sizeof(int64_t)), &entry._key, key_size);
	
	if (value_size != 0)
		memmove((p += sizeof(int64_t)), &entry._value[0], value_size);

	return move(v);

}


vector<char> serialize_log_entry_position(const log_entry_position& position) {

	vector<char> v(log_entry_position_store_size);

	auto p = &v[0];

	memmove(p, &position._file_i, sizeof(log_entry_position::_file_i));

	memmove((p += sizeof(log_entry_position::_file_i)), &position._offset, sizeof(log_entry_position::_offset));

	return move(v);

}


log_entry_position e_serialize_log_entry_position(const vector<char>& v) {

	auto p = &v[0];

	auto file_i = *reinterpret_cast<const uint32_t*>(p);

	auto offset = *reinterpret_cast<const int64_t*>((p += sizeof(log_entry_position::_file_i)));

	return log_entry_position(file_i, offset);

}


log_entry_tag e_serialize_log_entry_tag(const vector<char>& v) {

	auto p = &v[0];

	auto cyclic = *reinterpret_cast<const uint32_t*>(p);

	auto type = *reinterpret_cast<const log_entry::log_entry_type*>((p += sizeof(log_entry_tag::_cyclic)));

	auto v_size = *reinterpret_cast<const uint32_t*>((p += sizeof(log_entry_tag::_type)));

	return log_entry_tag(cyclic, v_size, type);

}


tuple<uint64_t, int64_t, vector<char>> e_serialize_n_k_v(const vector<char>& v, uint32_t value_size) {

	return make_tuple(*reinterpret_cast<const uint64_t*>(&v[0]), *reinterpret_cast<const int64_t*>(&v[0 + transaction_number_size]), vector<char>(v.begin() + transaction_number_size + key_size, v.end()));

}




