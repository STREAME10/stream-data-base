#pragma once

#include <cstdint>




































enum error_enum :int {

	error_null = 0,

	error = -1,

	error_stream_state_eof = -2,

	error_stream_state_fail = -4,

	error_file_corrupt = -6,

	error_fail_to_write = 8,

	error_fail_to_synchronize = -10,

	error_file_not_exist = -12,


	error_fail_to_create_container = -20,

	error_fail_to_create_io_manager = -22,

	error_key_not_exist = -30,


	error_is_merging = -40,

	error_insufficient_merge_file_space = -42,

	error_merger_aim_files_is_empty = -44,

	error_merge_complete_file_not_exist = -46,

	error_hint_file_not_exist = -48,

	error_merge_fail_to_initialize_path = -50,

	error_merge_fail_to_initialize_pipeline_number = -52,

	error_merge_engine_merger_is_nullptr = -54,

	error_merge_futures_is_empty = -56, 

	
	error_container_erase_key_is_empty = -100,

};











