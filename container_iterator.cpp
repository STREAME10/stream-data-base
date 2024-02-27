#include "container.h"






































_b_tree_iterator::_b_tree_iterator(const _b_tree& tree) {

	for (const auto& o : tree._impl)
		_values.push_back(o);

	_values.reserve(tree._impl.size());

	sort(_values.begin(), _values.end(),
		[](const pair<int64_t, log_entry_position>& w, const pair<int64_t, log_entry_position>& v) {
			return w.first < v.first;
		});

	_i = _values.begin();

}


void _b_tree_iterator::next() {

	++_i;

}


void _b_tree_iterator::rewind() {

	_i = _values.begin();

}


void _b_tree_iterator::seek(int64_t key) {

	_i = lower_bound(_values.begin(), _values.end(), key,
		[](const pair<int64_t, log_entry_position>& a, int64_t key) {
			return a.first < key;
		});

}


int64_t _b_tree_iterator::size()const {

	return static_cast<int64_t>(_values.size());

}


int64_t _b_tree_iterator::key()const {

	return _i->first;

}


log_entry_position _b_tree_iterator::value()const {

	return _i->second;

}


bool _b_tree_iterator::valid()const {

	return _i != _values.end();

}


black_tree_iterator::black_tree_iterator(const black_tree& tree) :
	_map(tree._impl), _i(tree._impl.begin()) {

}


void black_tree_iterator::next() {

	++_i;

}


void black_tree_iterator::rewind() {

	_i = _map.begin();

}


void black_tree_iterator::seek(int64_t key) {

	while (_i != _map.end() && _i->first < key)
		++_i;

}


int64_t black_tree_iterator::size()const {

	return static_cast<int64_t>(_map.size());

}


int64_t black_tree_iterator::key()const {

	return _i->first;

}


log_entry_position black_tree_iterator::value()const {

	return _i->second;

}


bool black_tree_iterator::valid()const {

	return _i != _map.end();

}


hash_table_iterator::hash_table_iterator(const hash_table& table) {

	for (const auto& o : table._impl)
		_values.push_back(o);

	_values.reserve(table._impl.size());

	sort(_values.begin(), _values.end(),
		[](const pair<int64_t, log_entry_position>& w, const pair<int64_t, log_entry_position>& v) {
			return w.first < v.first;
		});

	_i = _values.begin();

}


void hash_table_iterator::next() {

	++_i;

}


void hash_table_iterator::rewind() {

	_i = _values.begin();

}


void hash_table_iterator::seek(int64_t key) {

	_i = lower_bound(_values.begin(), _values.end(), key,
		[](const pair<int64_t, log_entry_position>& a, int64_t key) {
			return a.first < key;
		});

}


int64_t hash_table_iterator::size()const {

	return static_cast<int64_t>(_values.size());

}


int64_t hash_table_iterator::key()const {

	return _i->first;

}


log_entry_position hash_table_iterator::value()const {

	return _i->second;

}


bool hash_table_iterator::valid()const {

	return _i != _values.end();

}