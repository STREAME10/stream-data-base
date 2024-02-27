#pragma once

#include "io_manager.h"




































#include <functional>

class file;

class hint_file;


class file {

public:

	file(uint32_t, int64_t, io_manager::io_manager_type, const file_system::path&);

	file(uint32_t, int64_t, shared_ptr<io_manager>);

	tuple<optional<log_entry>, optional<int64_t>, int> read(int64_t);

	tuple<optional<vector<char>>, optional<int64_t>, int> read(int64_t, int64_t);

	int write(const vector<char>&);

	int synchronize();

	int close();

	inline auto file_i()const;

	inline auto write_offset()const;

	inline auto set_write_offset(int64_t);

	inline auto size();

	inline auto path()const;

	int for_each(function<int(const log_entry&)>);

	int for_each(function<int(const log_entry_position&, const log_entry&)>);

	int print();

private:

	const uint32_t _file_i;

	int64_t _write_offset;

	shared_ptr<io_manager> _io_manager;

};


auto file::file_i()const {

	return _file_i;

}


auto file::write_offset()const {

	return _write_offset;

}


auto file::set_write_offset(int64_t write_offset) {

	_write_offset = write_offset;

}


auto file::size() {

	return _io_manager->size();

}


auto file::path()const {

	return _io_manager->path();

}


class hint_file {

public:

	hint_file(uint32_t, int64_t, io_manager::io_manager_type, const file_system::path&);

	tuple<optional<hint>, optional<int64_t>, int> read(int64_t);

	int write(const hint&);

	inline auto synchronize();

	inline auto close();

	inline auto file_i()const;

	inline auto write_offset()const;

	inline auto set_write_offset(int64_t);

	inline auto size();

	inline auto path()const;

	int for_each(function<int(const hint&)>);

	int for_each(function<int(const log_entry_position&, const hint&)>);

	int print();

private:

	file _file;

};


auto hint_file::synchronize() {

	return _file.synchronize();

}


auto hint_file::close() {

	return _file.close();

}


auto hint_file::file_i()const {

	return _file.file_i();

}


auto hint_file::write_offset()const {

	return _file.write_offset();

}


auto hint_file::set_write_offset(int64_t write_offset) {

	return _file.set_write_offset(write_offset);

}


auto hint_file::size() {

	return _file.size();

}


auto hint_file::path()const {

	return _file.path();

}


class merge_complete_file {

public:

	merge_complete_file(io_manager::io_manager_type, const file_system::path&);

	tuple<optional<hint>, optional<int64_t>, int> read(int64_t);

	int write(const hint&);

	inline auto synchronize();

	inline auto close();

	inline auto file_i()const;

	inline auto write_offset()const;

	inline auto set_write_offset(int64_t);

	inline auto size();

	inline auto path()const;

	int for_each(function<int(const hint&)>);

	int for_each(function<int(const log_entry_position&, const hint&)>);

	int print();

private:

	file _file;

};



