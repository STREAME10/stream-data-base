#pragma once

#include "log.h"

#include "map.h"

#include "btree.h"

#include "error.h"






























#include <algorithm>

#include <unordered_map>

#include <map>

#include <mutex>

#include <thread>

#include <tuple>

#include <vector>

#include <optional>



class container;

class container_iterator;

class black_tree;

class black_tree_iterator;

class hash_table;

class hash_table_iterator;

class balance_tree;

class balance_tree_iterator;


class container {

public:

	enum container_type {

		black_tree_type = 0,

		hash_table_type = 1,

		b_tree_type = 2,

	};

	static shared_ptr<container> create_container(container_type);

public:

	virtual ~container() {};

	virtual int insert(int64_t, log_entry_position) = 0;

	virtual int erase(int64_t) = 0;

	virtual tuple<optional<log_entry_position>, int> get(int64_t)const = 0;

	virtual shared_ptr<container_iterator> iterator() = 0;

	virtual void print()const = 0;

};


class container_iterator {

public:

	virtual ~container_iterator() {};

	virtual void next() = 0;

	virtual void rewind() = 0;

	virtual void seek(int64_t) = 0;

	virtual int64_t size()const = 0;

	virtual int64_t key()const = 0;

	virtual log_entry_position value()const = 0;

	virtual bool valid()const = 0;

};


class _b_tree :public container {

	friend class _b_tree_iterator;

public:

	int insert(int64_t, log_entry_position);

	int erase(int64_t);

	tuple<optional<log_entry_position>, int> get(int64_t)const;

	shared_ptr<container_iterator> iterator();

	void print()const;

private:

	btree::map<int64_t, log_entry_position> _impl;

	mutex _mutex;

};


class _b_tree_iterator :public container_iterator {

	friend class _b_tree;

public:

	void next();

	void rewind();

	void seek(int64_t);

	int64_t size()const;

	int64_t key()const;

	log_entry_position value()const;

	bool valid()const;

private:

	_b_tree_iterator(const _b_tree&);

private:

	vector<pair<int64_t, log_entry_position>> _values;

	vector<pair<int64_t, log_entry_position>>::const_iterator _i;

};


class black_tree :public container {

	friend class black_tree_iterator;

public:

	int insert(int64_t, log_entry_position);

	int erase(int64_t);

	tuple<optional<log_entry_position>, int> get(int64_t)const;

	shared_ptr<container_iterator> iterator();

	void print()const;

private:

	map<int64_t, log_entry_position> _impl;

	mutex _mutex;

};


class black_tree_iterator :public container_iterator {

	friend class black_tree;

public:

	void next();

	void rewind();

	void seek(int64_t);

	int64_t size()const;

	int64_t key()const;

	log_entry_position value()const;

	bool valid()const;

private:

	black_tree_iterator(const black_tree&);

private:

	const map<int64_t, log_entry_position>& _map;

	map<int64_t, log_entry_position>::const_iterator _i;

};


class hash_table :public container {

	friend class hash_table_iterator;

public:

	int insert(int64_t, log_entry_position);

	int erase(int64_t);

	tuple<optional<log_entry_position>, int> get(int64_t)const;

	shared_ptr<container_iterator> iterator();

	void print()const;

private:

	unordered_map<int64_t, log_entry_position> _impl;

	mutex _mutex;

};


class hash_table_iterator :public container_iterator {

	friend class hash_table;

public:

	void next();

	void rewind();

	void seek(int64_t);

	int64_t size()const;

	int64_t key()const;

	log_entry_position value()const;

	bool valid()const;

private:

	hash_table_iterator(const hash_table&);

private:

	vector<pair<int64_t, log_entry_position>> _values;

	vector<pair<int64_t, log_entry_position>>::const_iterator _i;

};






