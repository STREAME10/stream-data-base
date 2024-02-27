#include "container.h"






































shared_ptr<container> container::create_container(container_type type) {

	switch (type) {
	case container_type::black_tree_type:
		return shared_ptr<container>(new black_tree);
	case container_type::hash_table_type:
		return shared_ptr<container>(new hash_table);
	case container_type::b_tree_type:
		return shared_ptr<container>(new _b_tree);
	default:
		return shared_ptr<container>(new _b_tree);
	}

}


int _b_tree::insert(int64_t key, log_entry_position position) {

	_impl.insert(make_pair(key, position));

	return 1;

}


int _b_tree::erase(int64_t key) {

	int erase_size = _impl.erase(key);

	if (erase_size != 1)
		return error_container_erase_key_is_empty;
	else
		return 0;

}


tuple<optional<log_entry_position>, int> _b_tree::get(int64_t key)const {

	auto i = _impl.find(key);

	if (i == _impl.end())
		return make_tuple(nullopt, error_key_not_exist);
	else
		return make_tuple(make_optional(i->second), 0);

}


int black_tree::insert(int64_t key, log_entry_position position)
{

	unique_lock<mutex> lock(_mutex);

	_impl.insert_or_assign(key, position);

	return 0;

}


int black_tree::erase(int64_t key)
{

	unique_lock<mutex> lock(_mutex);

	int erase_size = _impl.erase(key);

	if (erase_size != 1)
		return error_container_erase_key_is_empty;
	else
		return 0;

}


tuple<optional<log_entry_position>, int> black_tree::get(int64_t key)const
{

	auto i = _impl.find(key);

	if (i == _impl.end())
		return make_tuple(nullopt, error_key_not_exist);
	else
		return make_tuple(make_optional(i->second), 0);
	
}


int hash_table::insert(int64_t key, log_entry_position position)
{

	unique_lock<mutex> lock(_mutex);

	_impl.insert_or_assign(key, position);
	
	return 0;

}


int hash_table::erase(int64_t key) {

	unique_lock<mutex> lock(_mutex);

	int erase_size = _impl.erase(key);

	if (erase_size != 1)
		return error_container_erase_key_is_empty;
	else
		return 0;

	return 0;

}


tuple<optional<log_entry_position>, int> hash_table::get(int64_t key)const {

	auto i = _impl.find(key);

	if (i == _impl.end())
		return make_tuple(nullopt, error_key_not_exist);
	else
		return make_tuple(make_optional(i->second), 0);

}


shared_ptr<container_iterator> _b_tree::iterator() {
	
	unique_lock<mutex> lock(_mutex);

	return shared_ptr<container_iterator>(new _b_tree_iterator(*this));

}


shared_ptr<container_iterator> black_tree::iterator() {

	unique_lock<mutex> lock(_mutex);

	return shared_ptr<container_iterator>(new black_tree_iterator(*this));

}


shared_ptr<container_iterator> hash_table::iterator() {

	unique_lock<mutex> lock(_mutex);

	return shared_ptr<container_iterator>(new hash_table_iterator(*this));

}



