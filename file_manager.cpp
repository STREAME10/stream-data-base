#include "file_manager.h"






































file_manager::file_manager(file_system::path path, io_manager::io_manager_type type) :
	_path(path), _data_file_folder_path(_path / _data_file_folder_name), _merge_file_folder_path(_path / _merge_file_folder_name), _io_manager_type(type) {

	if (file_system::exists(_data_file_folder_path) == false)
		if (file_system::create_directory(_data_file_folder_path) == false)
			terminate();

	if (file_system::exists(_merge_file_folder_path) == false)
		if (file_system::create_directory(_merge_file_folder_path) == false)
			terminate();

}


tuple<optional<shared_ptr<file>>, int> file_manager::create_file(file_system::path file_path, uint32_t file_i)const {

	return make_tuple(make_optional(shared_ptr<file>(new file(file_i, 0, io_manager::create_io_manager(_io_manager_type, file_path)))), 0);

}


tuple<optional<vector<uint32_t>>, int> file_manager::get_file_is(file_system::path path, file_system::path suffix)const {

	vector<uint32_t> v;

	for (auto& i : file_system::directory_iterator(path)) {

		if (i.path().extension() != suffix)
			continue;

		uint32_t file_i = 0;

		try {

			file_i = stoi(i.path().filename());

		}
		catch (exception& e) {

			return make_tuple(nullopt, error_file_corrupt);

		}

		v.push_back(file_i);

	}

	return make_tuple(make_optional(move(v)), 0);

}























/*

tuple<optional<shared_ptr<file>>, int> file_manager::create_merge_complete_file()const {

	return make_tuple(make_optional(shared_ptr<file>(new file(0, 0, io_manager::create_io_manager(_io_manager_type, file_system::path(_merge_file_folder_path / _merge_complete_file_name))))), 0);

}


tuple<optional<shared_ptr<file>>, int> file_manager::open_merge_complete_file()const {

	for (auto& i : file_system::directory_iterator(_merge_file_folder_path)) {

		if (i.path().filename() == _hint_file_name) {

			return make_tuple(make_optional(shared_ptr<file>(new file(0, 0, io_manager::create_io_manager(_io_manager_type, file_system::path(_merge_file_folder_path / _merge_complete_file_name))))), 0);
		
		}

	}

	return make_tuple(nullopt, error);

}

*/