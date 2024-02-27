#pragma once

#include "io_manager.h"

#include "file.h"

#include "file_manager.h"

#include "container.h"

#include "log.h"




























using namespace std;

#include <thread>

#include <functional>

#include <future>

class engine_merger_pipeline;

class engine_merger;

class engine_iterator;

class engine;

class transaction;


class transaction {

public:

	friend class engine;

	struct transaction_options {

		uint32_t _capacity;

		bool _synchronize_write;

	};

	typedef transaction_options options;

	int insert(int64_t, vector<char>);

	int erase(int64_t key);

	int commit();

	void print()const;

private:

	int insert(int64_t, log_entry);

	transaction(engine&, transaction::options);


private:

	engine& _engine;

	transaction::options _options;

	map<int64_t, log_entry> _tasks;

	mutex _mutex;

};

const uint64_t transaction_number_un_transaction = 0;

const uint64_t transaction_number_unassignable = 0;


class engine {

public:

	friend class transaction;
	
	friend class engine_iterator;

	friend class engine_merger;

	friend class engine_merger_pipeline;

	struct engine_options {

		file_system::path _path;

		uint64_t _capacity;

		bool _synchronize_write;

		container::container_type _container_type;

		io_manager::io_manager_type _io_manager_type;

	};

	typedef engine_options options;

public:

	engine(engine::options);

	engine_iterator create_iterator()const;

	transaction create_transaction(transaction::options);

	int insert(int64_t, const vector<char>&);

	int erase(int64_t);

	tuple<optional<vector<char>>, int> get(int64_t)const;

	vector<int64_t> list_keys()const;

	int for_each(function<int(int64_t, const vector<char>&)>);

	int print()const;

	int initialize_merger();

	int merge();



private:

	tuple<optional<log_entry_position>, int> insert_log(const log_entry&);

	tuple<optional<log_entry_position>, int> insert_log_lock(const log_entry&);

	tuple<optional<vector<char>>, int> get(const log_entry_position&)const;

	tuple<optional<shared_ptr<file>>, int> search_file(uint32_t)const;

	int set_active_file();

	int load_a();

	int load_i();

	int load_i(const shared_ptr<file>&, map<uint64_t, list<tuple<log_entry, log_entry_position>>>&);

	int load_i(const log_entry&, const log_entry_position&);

private:

	options _options;

	shared_ptr<container> _container;

	shared_ptr<file_manager> _file_manager;

	shared_ptr<file> _active_file;

	map<uint32_t, shared_ptr<file>> _files;

	atomic<uint64_t> _transaction_number;

	unique_ptr<engine_merger> _merger;

	mutable mutex _mutex;

};


class engine_iterator {

public:

	engine_iterator(const engine&);

	inline auto rewind();

	inline auto next();

	inline auto seek(int64_t);

	inline int64_t size()const;

	inline int64_t key()const;

	inline tuple<optional<vector<char>>, int>  value()const;

	inline auto valid()const;

private:

	const engine& _engine;

	shared_ptr<container_iterator> _container_iterator;

};


auto engine_iterator::rewind() {

	_container_iterator->rewind();

}


auto engine_iterator::next() {

	_container_iterator->next();

}


auto engine_iterator::seek(int64_t key) {

	_container_iterator->seek(key);

}


int64_t engine_iterator::size()const {

	return _container_iterator->size();

}

int64_t engine_iterator::key()const {

	return _container_iterator->key();

}


tuple<optional<vector<char>>, int> engine_iterator::value()const {

	unique_lock<mutex> lock(_engine._mutex);

	return _engine.get(_container_iterator->value());

}


auto engine_iterator::valid()const {

	return _container_iterator->valid();

}


class engine_merger {

	friend class engine_merger_pipeline;

public:	

	struct options {

		options(file_system::path, io_manager::io_manager_type, uint64_t);

		file_system::path _path;

		io_manager::io_manager_type _io_manager_type;

		uint64_t _merge_file_capacity;

	};

	engine_merger(engine_merger::options, engine&, vector<shared_ptr<file>>);

	tuple<optional<list<future<int>>>, int> operator()();
	
	int print();

	int initialize();
private:
	int initialize_path();

	int initialize_pipeline();

	int initialize_pipeline_number();

	engine_merger_pipeline create_pipeline();

private:

	uint32_t _pipelines_number;

	vector<engine_merger_pipeline> _pipelines;

	int change_aim_file(shared_ptr<file>&);

	int change_merge_file(shared_ptr<file>&);

	int change_hint_file(shared_ptr<hint_file>&);

private:

	const options _options;
	
	engine& _engine;
	
	uint32_t _aim_file_i;

	uint32_t _merge_file_i;

	uint32_t _hint_file_i;

	vector<shared_ptr<file>> _aim_files;

	vector<shared_ptr<file>> _merge_files;

	vector<shared_ptr<hint_file>> _hint_files;

	mutable mutex _mutex_for_aim_files;

	mutable mutex _mutex_for_merge_files;

	mutable mutex _mutex_for_hint_files;

	shared_ptr<merge_complete_file> _merge_complete_file;

	inline static const file_system::path _merge_file_suffix = file_system::path(_chars(.merge));

	inline static const file_system::path _merge_complete_file_name = file_system::path(_chars(merge_complete));

	inline static const file_system::path _hint_file_suffix = file_system::path(_chars(.hint));

};


class engine_merger_pipeline {

public:

	engine_merger_pipeline(engine_merger&);

	engine_merger_pipeline(engine_merger&, shared_ptr<container>, shared_ptr<file>, shared_ptr<file>, shared_ptr<hint_file>);

	int operator()();

private:

	int insert(const log_entry&);

	inline int change_aim_file();

	inline int change_merge_file();

	inline int change_hint_file();

private:

	engine_merger& _engine_merger;

	shared_ptr<container> _engine_container;

	shared_ptr<file> _aim_file;

	shared_ptr<file> _merge_file;

	shared_ptr<hint_file> _hint_file;

};


int engine_merger_pipeline::change_aim_file() {

	return _engine_merger.change_aim_file(this->_aim_file);

}


int engine_merger_pipeline::change_merge_file() {

	return _engine_merger.change_merge_file(this->_merge_file);

}


int engine_merger_pipeline::change_hint_file() {

	return _engine_merger.change_hint_file(this->_hint_file);

}