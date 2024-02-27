#include "io_manager.h"






































io_manager::io_manager(file_system::path p) :
	_path(p)
{

}


shared_ptr<io_manager> io_manager::create_io_manager(io_manager_type type, file_system::path path) {

	switch (type) {
	case io_manager_type::file_io_type:
		return shared_ptr<io_manager>(new file_io(path));
	default:
		return shared_ptr<io_manager>(new file_io(path));
	}

}


file_io::file_io(file_system::path file_path) :
	io_manager(file_path), _file(file_path, ios::binary | ios::app | ios::in) {

}


tuple<optional<vector<char>>, int> file_io::read(int64_t o, int64_t size) {

	vector<char> v(size);

	_file.seekg(o);

	_file.read(&v[0], v.size());
	
	if (_file.fail()) {
		
		if (_file.eof()) {

			_file.clear();

			return make_tuple(nullopt, error_stream_state_eof);

		}
		else {

			return make_tuple(nullopt, error_stream_state_fail);

		}

	}

	return make_tuple(make_optional(move(v)), 0);

}


int file_io::write(const vector<char>& v) {

	_file.write(&v[0], v.size());

	if (_file.fail())
		return error_stream_state_fail;

	return 0;

}


int file_io::synchronize() {

	if (_file.sync() != 0)
		return error_fail_to_synchronize;

	return 0;

}


int file_io::close() {

	_file.close();

	return 1;

}


uint64_t file_io::size() {

	return static_cast<uint64_t>(_file.tellp());

}
