#include "engine.h"






































transaction::transaction(engine& engine, transaction::options op) :
	_engine(engine), _options(op) {

}


int transaction::insert(int64_t key, vector<char> value) {

	unique_lock<mutex> lock(_mutex);

	_tasks.insert_or_assign(key, log_entry::create_log_entry_normal(transaction_number_unassignable, key, move(value)));

	return 0;

}


int transaction::insert(int64_t key, log_entry entry) {

	_tasks.insert_or_assign(key, entry);

	if (_tasks.size() > this->_options._capacity)
		return error;

	return 0;

}


int transaction::erase(int64_t key) {

	unique_lock<mutex> lock(_mutex);

	auto [_, e] = _engine._container->get(key);

	if (e == 0) {

		if ((e = insert(key, log_entry::create_log_entry_erase(transaction_number_unassignable, key))) < 0) {

			return e;

		}

	}
	else if (e == (error_key_not_exist) && _tasks.contains(key)) {

		_tasks.erase(key);

	}

	return 0;

}


int transaction::commit() {

	unique_lock<mutex> lock_transaction(_mutex);

	if (_tasks.empty())
		return 0;

	if (static_cast<int64_t>(_tasks.size()) > _options._capacity)
		return error;

	unique_lock<mutex> lock_engine(_engine._mutex);

	auto assignable_transaction_number = (_engine._transaction_number += 1);

	map<int64_t, log_entry_position> positions;

	for (auto& [key, log_entry] : _tasks) {

		log_entry._transaction_number = assignable_transaction_number;

		auto [position, e] = _engine.insert_log(log_entry);

		if (e < 0)
			return e;

		positions.emplace(pair<int64_t, log_entry_position>(key, position.value()));

	}

	auto [_, e_c] = _engine.insert_log(log_entry::create_log_entry_complete(assignable_transaction_number));

	if (e_c < 0)
		return e_c;

	auto e_s = 0;

	if (_options._synchronize_write)
		if (_engine._active_file != nullptr)
			if ((e_s = _engine._active_file->synchronize()) < 0)
				return e_s;

	for (const auto& [key, log_entry] : _tasks)
		if (log_entry._type == log_entry::normal)
			_engine._container->insert(key, positions[key]);
		else if (log_entry._type == log_entry::erase)
			_engine._container->erase(key);
	
	return 0;

}
