#include "engine.h"






































engine::engine(options op) : 
	_options(op), _container(container::create_container(op._container_type)), _active_file(nullptr), _files(), _mutex() {
	
	_file_manager = shared_ptr<file_manager>(new file_manager(op._path, op._io_manager_type));

	if (load_a() < 0)
		terminate();

	if (load_i() < 0)
		terminate();

}


int engine::load_a() {

	auto [o_g, e_g] = _file_manager->get_data_file_is();

	if (e_g < 0)
		return -1;

	auto file_is = move(o_g.value());

	if (file_is.empty())
		return 0;

	sort(file_is.begin(), file_is.end());

	for (auto i = 0; i < file_is.size() - 1; ++i) {

		auto [o_a, e_a] = _file_manager->create_data_file(file_is[i]);
		
		if (e_a < 0)
			return -1;

		_files[file_is[i]] = move(o_a.value());
		
	}

	auto [o_c, e_c] = _file_manager->create_data_file(file_is[file_is.size() - 1]);

	if (e_c < 0)
		return -1;

	_active_file = move(o_c.value());

	return 0;

}


int engine::load_i() {

	int e = 0;

	if (nullptr == _active_file)
		return 0;

	map<uint64_t, list<tuple<log_entry, log_entry_position>>> transactions;
	
	for (const auto& [_, file] : _files)
		load_i(file, transactions);

	if ((e = load_i(_active_file, transactions)) < 0)
		return e;

	return 0;

}


int engine::load_i(const shared_ptr<file>& file, map<uint64_t, list<tuple<log_entry, log_entry_position>>>& transactions) {

	int64_t offset = 0;

	while (1) {

		auto [entry, size, e] = file->read(offset);

		if (e < 0)
			if (e == (-1 - 1))
				break;
			else
				return e;

		if (entry->relate_transaction()) {

			if (entry->_type == log_entry::complete) {

				for (const auto& [transaction_entry, transaction_entry_position] : transactions[entry->_transaction_number]) {

					load_i(transaction_entry, transaction_entry_position);

				}

				transactions.erase(entry->_key);

			}
			else {

				transactions[entry->_transaction_number].emplace_back(entry.value(), log_entry_position(file->file_i(), offset));

			}

			if (_transaction_number < entry->_transaction_number) {

				_transaction_number = entry->_transaction_number;

			}

		}
		else if ((e = load_i(entry.value(), log_entry_position(file->file_i(), offset))) < 0) {

			return e;

		}

		offset += size.value();
		
		file->set_write_offset(offset);

	}

	return 0;

};


int engine::load_i(const log_entry& entry, const log_entry_position& position) {

	auto e = 0;

	if (entry._type == log_entry::normal)
		e = _container->insert(entry._key, position);
	else if (entry._type == log_entry::erase)
		e = _container->erase(entry._key);
	else
		return -1;

	return e;

}