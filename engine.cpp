#include "engine.h"






































int engine::set_active_file() {

	auto active_file_i = 0;

	if (_active_file != nullptr)
		active_file_i = _active_file->file_i() + 1;

	auto [o, e] = _file_manager->create_data_file(active_file_i);

	if (e < 0)
		return e;

	_active_file = move(o.value());

	return 0;

}


tuple<optional<shared_ptr<file>>, int> engine::search_file(uint32_t file_i)const {

	if (file_i == _active_file->file_i())
		return make_tuple(make_optional(_active_file), 0);

	auto i = _files.find(file_i);

	if (i == _files.end())
		return make_tuple(nullopt, error_file_not_exist);

	return make_tuple(make_optional(i->second), 0);

}


vector<int64_t> engine::list_keys()const {

	auto i = _container->iterator();

	vector<int64_t> keys;

	keys.reserve(i->size());

	for (i->rewind(); i->valid(); i->next())
		keys.emplace_back(i->key());

	return move(keys);

}


int engine::for_each(function<int(int64_t, const vector<char>&)> func) {

	unique_lock<mutex> lock(_mutex);

	for (auto i = _container->iterator(); i->valid(); i->next()) {

		auto [v, e] = get(i->value());

		if (e < 0)
			return e;

		if (func(i->key(), v.value()) == false)
			break;

	}

	return 0;

}


engine_iterator engine::create_iterator()const {

	return engine_iterator(*this);

};


transaction engine::create_transaction(transaction::options o) {

	return transaction(*this, o);

}