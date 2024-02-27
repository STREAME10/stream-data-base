#include "engine.h"






































int engine::insert(int64_t key, const vector<char>& value) {

	auto [position, e] = insert_log(log_entry::create_log_entry_normal(0, key, value));

	if (e < 0)
		return e;

	if ((e = _container->insert(key, position.value())) < 0)
		return e;

	return 0;

}


tuple<optional<log_entry_position>, int> engine::insert_log(const log_entry& entry) {

	int e = 0;

	if (_active_file == nullptr)
		if ((e = set_active_file()) < 0)
			return make_tuple(nullopt, e);

	auto s = serialize_log_entry(entry);

	if (_active_file->write_offset() + s.size() > _options._capacity) {

		if (s.size() > _options._capacity)
			return make_tuple(nullopt, error);

		if ((e = _active_file->synchronize()) < 0)
			return make_tuple(nullopt, e);

		_files[_active_file->file_i()] = _active_file;

		if ((e = set_active_file()) < 0)
			return make_tuple(nullopt, e);

	}

	auto offset = _active_file->write_offset();

	if ((e = _active_file->write(s)) < 0)
		return make_tuple(nullopt, e);

	if (_options._synchronize_write == true)
		if ((e = _active_file->synchronize()) < 0)
			return make_tuple(nullopt, e);

	return make_tuple(make_optional(log_entry_position(_active_file->file_i(), offset)), 0);

}


tuple<optional<log_entry_position>, int> engine::insert_log_lock(const log_entry& entry) {

	unique_lock<mutex> lock(_mutex);

	return insert_log(entry);

}


int engine::erase(int64_t key) {

	int e = 0;

	tie(ignore, e) = _container->get(key);

	if (e < 0)
		return e;

	tie(ignore, e) = insert_log(log_entry::create_log_entry_erase(transaction_number_un_transaction, key));

	if (e < 0)
		return e;

	if ((e = _container->erase(key)) < 0)
		return e;

	return 0;

}


tuple<optional<vector<char>>, int> engine::get(int64_t key)const {

	unique_lock<mutex> lock(this->_mutex);

	auto [position, e] = _container->get(key);

	if (e < 0)
		return make_tuple(nullopt, e);

	return get(position.value());

}


tuple<optional<vector<char>>, int> engine::get(const log_entry_position& position)const {

	auto [o, e_s] = search_file(position._file_i);
	
	if (e_s < 0)
		return make_tuple(nullopt, e_s);

	auto i = move(o.value());
	
	auto [entry, entry_size, e] = i->read(position._offset);

	if (e < 0)
		return make_tuple(nullopt, e);

	if (entry->_type != log_entry::log_entry_type::normal)
		return make_tuple(nullopt, error);

	return make_tuple(make_optional(move(entry->_value)), 0);

}