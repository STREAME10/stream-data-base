#include "file.h"






































file::file(uint32_t file_i, int64_t write_offset, io_manager::io_manager_type type, const file_system::path& path) :
	_file_i(file_i), _write_offset(write_offset), _io_manager(io_manager::create_io_manager(type, path)) {

}


file::file(uint32_t file_i, int64_t write_offset, shared_ptr<io_manager> io) :
	_file_i(file_i), _write_offset(write_offset), _io_manager(move(io)) {

}


tuple<optional<log_entry>, optional<int64_t>, int> file::read(int64_t o) {

	auto [_tag, _tag_size, e_a] = read(o, log_entry_tag_store_size);

	if (e_a < 0)
		return make_tuple(nullopt, nullopt, e_a);

	auto tag = e_serialize_log_entry_tag(_tag.value());

	auto [_k, _k_size, e] = read((o += log_entry_tag_store_size), static_cast<int64_t>(transaction_number_size) + static_cast<int64_t>(key_size) + tag._v_size);

	if (e < 0)
		return make_tuple(nullopt, nullopt, e);

	auto [n, k, v] = e_serialize_n_k_v(_k.value(), tag._v_size);

	return make_tuple(make_optional(log_entry(n, k, v, tag._type)), make_optional(static_cast<int64_t>(log_entry_tag_store_size + transaction_number_size + key_size + tag._v_size)), 0);

}


tuple<optional<vector<char>>, optional<int64_t>, int> file::read(int64_t o, int64_t size) {
	
	auto [v, e] = _io_manager->read(o, size);

	if (e < 0)
		return make_tuple(nullopt, nullopt, e);

	return make_tuple(v, make_optional(size), e);

}


int file::write(const vector<char>& v) {

	int e = 0;
	
	if ((e = _io_manager->write(v)) < 0)                                                                                                                                                                                                        	if (e < 0)
		return e;

	_write_offset += v.size();

	return 0;

}


int file::synchronize() {

	return _io_manager->synchronize();

}


int file::close() {

	return _io_manager->close();

}


int file::for_each(function<int(const log_entry&)> func) {

	int64_t offset = 0;

	while (1) {

		auto [entry, entry_size, e] = this->read(offset);

		if (e < 0)
			if (e == (error_stream_state_eof))
				break;
			else
				return e;

		if ((e = func(entry.value())) < 0)
			return e;

		offset += entry_size.value();

	}

	return 0;

}


int file::for_each(function<int(const log_entry_position&, const log_entry&)> func) {

	int64_t offset = 0;

	while (1) {
		
		auto [entry, entry_size, e] = this->read(offset);
		
		if (e < 0)
			if (e == (error_stream_state_eof))
				break;
			else
				return e;

		if ((e = func(log_entry_position(this->file_i(), offset), entry.value())) < 0)
			return e;

		offset += entry_size.value();

	}

	return 0;

}


hint_file::hint_file(uint32_t file_i, int64_t write_offset, io_manager::io_manager_type type, const file_system::path& path) :
	_file(file_i, write_offset, type, path) {

}


tuple<optional<hint>, optional<int64_t>, int> hint_file::read(int64_t offset) {

	auto [entry, entry_size, e] = _file.read(offset);

	if (e < 0)
		return make_tuple(nullopt, nullopt, e);

	return make_tuple(hint(entry.value()), entry_size, e);

}


int hint_file::write(const hint& in) {
	
	return _file.write(serialize_hint(in));

}


int hint_file::for_each(function<int(const hint&)> func) {

	_file.for_each([func](const log_entry& entry) {

		return func(hint(entry));

		});

	return 0;

}


int hint_file::for_each(function<int(const log_entry_position&, const hint&)> func) {

	_file.for_each([func](const log_entry_position& entry_position, const log_entry& entry) {

		return func(entry_position, hint(entry));

		});

	return 0;

}