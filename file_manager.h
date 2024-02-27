#pragma once

#include "file.h"




































class file_manager {
	
public:

	file_manager(file_system::path = file_system::current_path(), io_manager::io_manager_type = io_manager::file_io_type);

	inline tuple<optional<shared_ptr<file>>, int> create_data_file(uint32_t)const;

	inline tuple<optional<shared_ptr<file>>, int> create_merge_file(uint32_t)const;

	inline tuple<optional<vector<uint32_t>>, int> get_data_file_is()const;

	inline tuple<optional<vector<uint32_t>>, int> get_merge_file_is()const;

private:

	inline file_system::path data_file_path(uint32_t)const;

	inline file_system::path merge_file_path(uint32_t)const;

	tuple<optional<shared_ptr<file>>, int> create_file(file_system::path, uint32_t)const;

	tuple<optional<vector<uint32_t>>, int> get_file_is(file_system::path, file_system::path)const;

private:

	const file_system::path _path;

	const file_system::path _data_file_folder_path;

	const file_system::path _merge_file_folder_path;

	inline static const file_system::path _data_file_folder_name = file_system::path(_chars(data));

	inline static const file_system::path _data_file_suffix = file_system::path(_chars(.data));

	inline static const file_system::path _merge_file_folder_name = file_system::path(_chars(merge));

	inline static const file_system::path _merge_file_suffix = file_system::path(_chars(.merge));
	
	inline static const file_system::path _merge_complete_file_name = file_system::path(_chars(merge_complete));

	inline static const file_system::path _hint_file_name = file_system::path(_chars(hint));

	const io_manager::io_manager_type _io_manager_type;

};


tuple<optional<shared_ptr<file>>, int> file_manager::create_data_file(uint32_t file_i) const {

	return create_file(data_file_path(file_i), file_i);

}


tuple<optional<shared_ptr<file>>, int> file_manager::create_merge_file(uint32_t file_i) const {

	return create_file(merge_file_path(file_i), file_i);

}


file_system::path file_manager::data_file_path(uint32_t file_i) const {

	return _data_file_folder_path / file_system::path(to_string(file_i)) += _data_file_suffix;

}


file_system::path file_manager::merge_file_path(uint32_t file_i) const {

	return _merge_file_folder_path / file_system::path(to_string(file_i)) += _merge_file_suffix;

}


tuple<optional<vector<uint32_t>>, int> file_manager::get_data_file_is() const {

	return get_file_is(_data_file_folder_path, _data_file_suffix);

}


tuple<optional<vector<uint32_t>>, int> file_manager::get_merge_file_is() const {

	return get_file_is(_merge_file_folder_path, _merge_file_suffix);

}