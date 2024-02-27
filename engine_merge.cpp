#include "engine.h"










































int engine::merge() {

	if (_merger == nullptr)
		return error_merge_engine_merger_is_nullptr;

	auto [o, e] = _merger->operator()();

	if (e < 0)
		return e;

	auto futures(move(o.value()));

	auto r = 0;

	while (!futures.empty()) {

		auto pooling_futures = [&futures, &r]() {

			auto i = futures.begin();

			while (i != futures.end())
				if (i->wait_for(chrono::duration<int64_t, nano>::zero()) == future_status::ready)
					if ((r = i->get()) < 0)
						return r;
					else
						i = futures.erase(i);
				else
					++i;

			return 0;

		};
	
		if ((e = pooling_futures()) < 0)
			return e;

	}

	return 0;

}


int engine::initialize_merger() {

	unique_lock<mutex> k(_mutex);

	int e = 0;

	if (_merger != nullptr)
		return error_is_merging;

	if (_active_file == nullptr)
		return 0;

	if ((e = _active_file->synchronize()) < 0)
		return e;

	auto final_aim_file_i = _active_file->file_i();

	_files[_active_file->file_i()] = _active_file;

	if ((e = set_active_file()) < 0)
		return e;

	vector<shared_ptr<file>> aim_files;

	aim_files.reserve(_files.size());

	for (const auto& [_, i] : _files)
		aim_files.emplace_back(i);

	_merger = unique_ptr<engine_merger>(new engine_merger(engine_merger::options(_options._path / _chars(merge), _options._io_manager_type, _options._capacity), *this, move(aim_files)));

	_merger->initialize();

	return 0;

}


engine_merger::engine_merger(engine_merger::options options_, engine& engine_, vector<shared_ptr<file>> aim_files_) :
	_options(options_), _engine(engine_), _aim_file_i(0), _merge_file_i(0), _hint_file_i(0), _aim_files(move(aim_files_)), _merge_files(), _hint_files(), _pipelines_number(0) {

}


engine_merger::options::options(file_system::path path, io_manager::io_manager_type type, uint64_t merge_file_capacity) :
	_path(path), _io_manager_type(type), _merge_file_capacity(merge_file_capacity) {
	
}


tuple<optional<list<future<int>>>, int> engine_merger::operator()() {

	list<future<int>> futures;

	for (auto& pipeline : _pipelines)
		futures.emplace_back(async(launch::async, pipeline));

	return make_tuple(optional<list<future<int>>>(move(futures)), 0);

}


int engine_merger::initialize() {

	auto e = 0;

	if ((e = initialize_path()) < 0)
		return e;

	return initialize_pipeline();

}


int engine_merger::initialize_path() {

	if (file_system::exists(_options._path) == false)
		if (file_system::create_directory(_options._path) == false)
			return error_merge_fail_to_initialize_path;

	return 0;

}


int engine_merger::initialize_pipeline() {

	auto e = 0;

	if ((e = initialize_pipeline_number()) < 0)
		if (e == error_merge_fail_to_initialize_pipeline_number)
			_pipelines_number = 1;
		else
			return e;

	_pipelines.reserve(_pipelines_number);

	for (auto i = 0; i < _pipelines_number; ++i)
		_pipelines.emplace_back(create_pipeline());

	return 0;

}


int engine_merger::initialize_pipeline_number() {

	_pipelines_number = min(static_cast<size_t>(thread::hardware_concurrency()), _aim_files.size());

	return 0;

}


engine_merger_pipeline engine_merger::create_pipeline() {

	return engine_merger_pipeline(*this);

}


int engine_merger::change_hint_file(shared_ptr<hint_file>& in) {

	unique_lock<mutex> lock(_mutex_for_hint_files);

	in = shared_ptr<hint_file>(new hint_file(_hint_file_i, 0, _options._io_manager_type, _options._path / to_string(_hint_file_i) += _hint_file_suffix));

	++_hint_file_i;

	_hint_files.emplace_back(in);

	return 0;

}


int engine_merger::change_aim_file(shared_ptr<file>& aim) {

	unique_lock<mutex> lock(_mutex_for_aim_files);

	if (_aim_file_i == this->_aim_files.size())
		return error_merger_aim_files_is_empty;

	aim = _aim_files[_aim_file_i];

	++_aim_file_i;

	return 0;

}


int engine_merger::change_merge_file(shared_ptr<file>& merge) {

	unique_lock<mutex> lock(_mutex_for_merge_files);

	merge = shared_ptr<file>(new file(_merge_file_i, 0, _options._io_manager_type, _options._path / to_string(_merge_file_i) += _merge_file_suffix));
	
	++_merge_file_i;

	_merge_files.emplace_back(merge);

	return 0;

}


engine_merger_pipeline::engine_merger_pipeline(engine_merger& merger) :
	_engine_merger(merger), _engine_container(merger._engine._container), _aim_file(nullptr), _merge_file(nullptr), _hint_file(nullptr) {

	this->change_aim_file();

	this->change_merge_file();

	this->change_hint_file();

}


engine_merger_pipeline::engine_merger_pipeline(engine_merger& merger, shared_ptr<container> container, shared_ptr<file> a_file, shared_ptr<file> g_file, shared_ptr<hint_file> in) :
	_engine_merger(merger), _engine_container(move(container)), _aim_file(move(a_file)), _merge_file(move(g_file)), _hint_file(move(in)) {

}


int engine_merger_pipeline::operator()() {

	int64_t o = 0;

	while (true) {

		auto [entry, entry_size, e] = _aim_file->read(o);
		std::cout << setw(10) << std::this_thread::get_id() << setw(10) << 1 << endl;
		if (e < 0)
			if (e == (error_stream_state_eof))
				if ((e = change_aim_file()) < 0)
					if (e == error_merger_aim_files_is_empty)
						return 0;
					else
						return e;
				else
					return this->operator()();
			else
				return e;

		log_entry_position r_position(_aim_file->file_i(), o);

		o += entry_size.value();

		if (entry->_type != log_entry::normal)
			continue;

		auto [c_position, e_c] = _engine_container->get(entry->_key);

		if (e_c < 0)
			if (e_c == error_key_not_exist)
				continue;
			else
				return e_c;

		if (r_position == c_position.value()) {

			if (entry->relate_transaction())
				entry->_transaction_number = transaction_number_un_transaction;

			if ((e = insert(entry.value())) < 0)
				return e;

		}

	}

	return 0;

}


int engine_merger_pipeline::insert(const log_entry& entry) {

	auto e = 0;

	auto serialize_entry = serialize_log_entry(entry);

	if (_merge_file->write_offset() + serialize_entry.size() > _engine_merger._options._merge_file_capacity)
		if ((e = change_merge_file()) < 0)
			return e;

	auto offset = _merge_file->write_offset();

	if ((e = _merge_file->write(serialize_entry)) < 0)
		return e;
	
	if ((e = _hint_file->write(hint(entry._key, log_entry_position(_merge_file->file_i(), offset)))) < 0)
		return e;

	return 0;

}

