#include "print.h"










#define new_line cout << endl



























void print(const vector<char>& v) {

	for (const auto& i : v)
		cout << setw(10) << bitset<8>(i);

	return;

}


void print(const int64_t& key) {

	cout << setw(10) << key;

	return;

}


void print(const log_entry::log_entry_type& type) {

	if (type == log_entry::normal)
		cout << setw(10) << _chars(normal);
	else if (type == log_entry::erase)
		cout << setw(10) << _chars(erase);
	else if (type == log_entry::complete)
		cout << setw(10) << _chars(complete);

	return;

}


void log_entry::print() const {

	::print(_transaction_number);

	::print(this->_type);
	
	::print(this->_key);
	
	::print(this->_value);

	new_line;

	return;

}


void log_entry_position::print() const {

	cout << setw(10) << this->_file_i << setw(10) << this->_offset;

	return;

}


void log_entry_tag::print() const {

	cout << setw(10) << this->_cyclic << setw(10) << this->_v_size;
	
	::print(this->_type);

	new_line;

	return;

}


void _b_tree::print()const {

	for (const auto& [key, position] : _impl) {

		::print(key), position.print();

		new_line;

	}

	return;


}


void black_tree::print() const {

	for (const auto& [key, position] : _impl) {

		::print(key), position.print();

		new_line;

	}

	return;

}


void hash_table::print() const {

	for (const auto& [key, position] : _impl) {

		::print(key);
		
		position.print();

		new_line;

	}

	return;

}


int file::print() {

	cout << setw(10) << this->_file_i;

	new_line;

	return this->for_each([](const log_entry_position& position, const log_entry& entry) {

		position.print();

		entry.print();

		return 1;

		});

}


void file_io::print()const {


}


void transaction::print()const {

	
	for (const auto& [key, entry] : _tasks) {

		::print(key);
		
		entry.print();

		new_line;

	}

};


int engine::print()const {
	
	for (const auto& [_, file] : _files)
		file->print();
	
	_active_file->print();

	new_line;

	return 1;

}

void hint::print()const {

	::print(this->_key);

	_position.print();

	new_line;

}


int hint_file::print() {

	return this->for_each([](const log_entry_position& entry_position, const hint& in) {

		entry_position.print();

		in.print();

		return 0;

		});

}


int engine_merger::print() {

	for (auto i : this->_aim_files)
		i->print();
	
	for (auto i : this->_merge_files)
		i->print();
	
	for (auto i : this->_hint_files)
		i->print();

	return 0;

}


