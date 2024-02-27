#include "log.h"

#include "print.h"

#include "container.h"

#include <chrono>



int main() {
	

	

	
	engine a(engine::options(file_system::current_path(), 1000000, true, container::b_tree_type, io_manager::file_io_type));
	
	
	auto time_insert_start = chrono::system_clock::now();

	for (int i = 0; i < 10000; ++i)
		a.insert(i, vector<char>(1024, 1));

	auto time_insert_complete = chrono::system_clock::now();

	auto time_get_start = chrono::system_clock::now();

	for (int i = 0; i < 10000; ++i)
		a.get(i);

	auto time_get_complete = chrono::system_clock::now();

	auto time_erase_start = chrono::system_clock::now();

	for (int i = 0; i < 10000; ++i)
		a.erase(i);

	auto time_erase_complete = chrono::system_clock::now();

	cout << setw(10 + 10 + 10) << time_insert_complete - time_insert_start << endl;
	
	cout << setw(10 + 10 + 10) << time_get_complete - time_get_start << endl;

	cout << setw(10 + 10 + 10) << time_erase_complete - time_erase_start << endl;
	

	return 1;

}