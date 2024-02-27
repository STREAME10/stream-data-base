/*
tuple<optional<shared_ptr<hint_file>>, int> engine::create_hint_file() {

	return _file_manager->create_hint_file();

}


tuple<optional<shared_ptr<file>>, int> engine::create_merge_file(uint32_t file_i) {

	return _file_manager->create_merge_file(file_i);

}


tuple<optional<shared_ptr<file>>, int> engine::create_merge_complete_file() {

	return _file_manager->create_merge_complete_file();

}


tuple<optional<shared_ptr<file>>, int> engine::open_merge_complete_file()const {

	return _file_manager->create_merge_complete_file();

}


int engine::merge() {

	struct is_merging_manager {

		atomic<int>& _is_merging;

		is_merging_manager(atomic<int>& is_merging) :
			_is_merging(is_merging) {

			this->_is_merging = true;

		}

		~is_merging_manager() {

			this->_is_merging = false;

		}

	};

	int e = 0;

	unique_lock<mutex> k(_mutex);

	if (_active_file == nullptr)
		return 0;

	if (_is_merging == true)
		return error_is_merging;

	is_merging_manager _is_merging_manager(_is_merging);

	if ((e = _active_file->synchronize()) < 0)
		return e;

	auto final_merge_file_i = _active_file->file_i();

	_files.insert(make_pair(_active_file->file_i(), _active_file));

	if ((e = set_active_file()) < 0)
		return e;

	vector<shared_ptr<file>> merge_files;

	merge_files.reserve(_files.size());

	for (const auto& [_, i] : _files)
		merge_files.emplace_back(i);

	k.unlock();

	sort(merge_files.begin(), merge_files.end(),
		[](const shared_ptr<file>& w, const shared_ptr<file>& v) {
			return w->file_i() < v->file_i();
		});

	auto [hint, e_c] = create_hint_file();

	if (e_c < 0)
		return e_c;

	auto hint_file = hint.value();

	merge_file(merge_files, hint_file);

	if ((e = hint_file->synchronize()) < 0)
		return e;
	
	hint_file->for_each([](const log_entry_position& p, const log_entry& e) {

		cout << endl;

		p.print();

		cout << setw(10) << e._key;

		e_serialize_log_entry_position(e._value).print();

		cout << endl;

		return 0;

		});

	system("pause"); 

	auto [complete, e_m] = create_merge_complete_file();

	if (e_m < 0)
		return e_m;

	auto merge_complete_file = complete.value();

	merge_complete_file->write(serialize_log_entry(log_entry(0, key_complete, serialize_i(final_merge_file_i), log_entry::normal)));

}


int engine::merge_file(const vector<shared_ptr<file>>& merge_files, shared_ptr<hint_file>& hint_file) {

	atomic<int> merge_file_i = 0;

	auto [merge_f, e_m] = this->create_merge_file(merge_file_i);

	if (e_m < 0)
		return e_m;

	auto merge_file = merge_f.value();

	for (const auto& i : merge_files) {

		int64_t o = 0;	

		while (true) {

			auto [entry, entry_size, e] = i->read(o);

			if (e < 0)
				if (e == (error_stream_state_eof))
					break;
				else
					return e;

			log_entry_position r_position(i->file_i(), o);

			o += entry_size.value();

			if (entry->_type != log_entry::normal)
				continue;

			auto [c_position, e_c] = _container->get(entry->_key);

			if (e_c < 0)
				if (e_c == error_key_not_exist)
					continue;
				else
					return e_c;

			if (r_position == c_position.value()) {
				
				if (entry->relate_transaction())
					entry->_transaction_number = transaction_number_un_transaction;

				tie(ignore, e) = merge_insert(entry.value(), merge_file, hint_file, merge_file_i);
			
				if (e < 0)
					return e;

			}
			
		}

	}

	return 0;

}

tuple<optional<log_entry_position>, int> engine::merge_insert(const log_entry& entry, shared_ptr<file>& merge_file, shared_ptr<hint_file>& hint_file, atomic<int>& merge_file_i) {

	auto [_ig, e] = merge_insert(entry, merge_file, hint_file);

	if (e < 0) {

		if (e == error_insufficient_merge_file_space) {

			++merge_file_i;

			auto [new_merge_file, e_m] = create_merge_file(merge_file_i);

			if (e_m < 0)
				return make_tuple(nullopt, e_m);

			merge_file = new_merge_file.value();

			return merge_insert(entry, merge_file, hint_file);

		}
		else
			return make_tuple(nullopt, e);
		
	}

	return make_tuple(_ig, 0);
}


tuple<optional<log_entry_position>, int> engine::merge_insert(const log_entry& entry, shared_ptr<file>& merge_file, shared_ptr<file>& hint_file) {

	auto e = 0;

	auto s = serialize_log_entry(entry);

	if (merge_file->write_offset() + s.size() > _options._capacity)
		return make_tuple(nullopt, error_insufficient_merge_file_space);

	auto offset = merge_file->write_offset();

	if ((e = merge_file->write(s)) < 0)
		return make_tuple(nullopt, e);

	if (_options._synchronize_write)
		if ((e = merge_file->synchronize()) < 0)
			return make_tuple(nullopt, e);

	log_entry_position position(merge_file->file_i(), offset);

	if ((e = hint_file->write(serialize_hint(entry._key, position))) < 0)
		return make_tuple(nullopt, e);

	return make_tuple(make_optional(position), 0);

}


int engine::merge_load() {

	auto [o, e] = _file_manager->open_merge_complete_file();

	if (e < 0)
		return e;

	auto merge_complete_file = o.value();

	auto [merge_information, _, e_a] = merge_complete_file->read(0);

	if (e_a < 0)
		return e_a;

	auto final_merge_file_i = e_serialize_i(merge_information->_value);

	unique_lock<mutex> lock(_mutex);

	for (int i = 0; i <= final_merge_file_i; ++i)
		file_system::remove(_files[i]->path());





	return 0;

}
*/

/*

int engine_merger::merge(shared_ptr<file>& aim, shared_ptr<file>& merge, shared_ptr<hint_file>& in) {

	int64_t o = 0;

	while (true) {

		auto [entry, entry_size, e] = aim->read(o);

		if (e < 0) {

			if (e == (error_stream_state_eof)) {

				e = change_aim_file(aim);

				if (e < 0) {

					if (e == error_merger_aim_files_is_empty)
						return 0;
					else
						return e;

				}

			}
			else {

				return e;

			}

		}

		log_entry_position r_position(aim->file_i(), o);

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

			tie(ignore, e) = insert(entry.value(), merge, in);

			if (e < 0)
				return e;

		}

	}

	return 0;

}

*/


/*

int engine_merger::merge(shared_ptr<file>& aim, shared_ptr<file>& merge, shared_ptr<hint_file>& in) {

		int64_t o = 0;

		while (true) {

			auto [entry, entry_size, e] = aim->read(o);

			if (e < 0) {

				if (e == (error_stream_state_eof)) {

					e = change_aim_file(aim);

					if (e < 0) {

						if (e == error_merger_aim_files_is_empty)
							return 0;
						else
							return e;

					}

				}
				else {

					return e;

				}

			}

			log_entry_position r_position(aim->file_i(), o);

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

				tie(ignore, e) = insert(entry.value(), merge, in);

				if (e < 0)
					return e;

			}

		}

	return 0;

}

tuple<optional<log_entry_position>, int> engine_merger::insert(const log_entry& entry, shared_ptr<file>& merge, shared_ptr<hint_file>& in) {

	auto e = 0;

	auto serialize_entry = serialize_log_entry(entry);

	if (merge->write_offset() + serialize_entry.size() > _merge_file_capacity) {

		e = this->change_merge_file(merge);

		if (e < 0)
			return make_tuple(nullopt, e);

	}

	auto o = merge->write_offset();

	if ((e = merge->write(serialize_entry)) < 0)
		return make_tuple(nullopt, e);

	log_entry_position position(merge->file_i(), o);

	if ((e = in->write(hint(entry._key, position))) < 0)
		return make_tuple(nullopt, e);

	return make_tuple(make_optional(position), 0);

}






/*
tuple<optional<log_entry_position>, int> engine_merger::insert(const log_entry& entry, shared_ptr<file>& merge, shared_ptr<hint_file>& in) {


	auto e = 0;

	auto s = serialize_log_entry(entry);

	if (merge->write_offset() + s.size() > _merge_file_capacity) {

		e = this->change_merge_file(merge);

		if (e < 0)
			return make_tuple(nullopt, e);

	}

	auto o = merge->write_offset();

	if ((e = merge->write(s)) < 0)
		return make_tuple(nullopt, e);

	log_entry_position position(merge->file_i(), o);

	if ((e = in->write(hint(entry._key, position))) < 0)
		return make_tuple(nullopt, e);

	return make_tuple(make_optional(position), 0);



	auto [_ig, e] = insert_(entry, merge_file, hint_file);

	if (e < 0) {

		if (e == error_insufficient_merge_file_space) {

			++merge_file_i;

			auto [new_merge_file, e_m] = create_merge_file(merge_file_i);

			if (e_m < 0)
				return make_tuple(nullopt, e_m);

			merge_file = new_merge_file.value();

			return merge_insert(entry, merge_file, hint_file);

		}
		else
			return make_tuple(nullopt, e);

	}

	return make_tuple(_ig, 0);

}


/*




int engine_merger::merge_file() {

	auto merge_file = this->create_merge_file(_merge_file_i);

	for (const auto& i : _aim_files) {

		int64_t o = 0;

		while (true) {

			auto [entry, entry_size, e] = i->read(o);

			if (e < 0)
				if (e == (error_stream_state_eof))
					break;
				else
					return e;

			log_entry_position r_position(i->file_i(), o);

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

				tie(ignore, e) = insert(entry.value(), merge_file, _hint_file);

				if (e < 0)
					return e;

			}

		}

	}

	return 0;

}


tuple<optional<log_entry_position>, int> engine_merger::insert(const log_entry& entry, shared_ptr<file>& aim_file, shared_ptr<file>& merge_file, shared_ptr<hint_file>& hint_file) {

	auto e = 0;

	auto s = serialize_log_entry(entry);

	if (merge_file->write_offset() + s.size() > _merge_file_capacity)
		return make_tuple(nullopt, error_insufficient_merge_file_space);

	auto offset = merge_file->write_offset();

	if ((e = merge_file->write(s)) < 0)
		return make_tuple(nullopt, e);

	if (_options._synchronize_write)
		if ((e = merge_file->synchronize()) < 0)
			return make_tuple(nullopt, e);

	log_entry_position position(merge_file->file_i(), offset);

	if ((e = hint_file->write(serialize_hint(entry._key, position))) < 0)
		return make_tuple(nullopt, e);

	return make_tuple(make_optional(position), 0);

}





tuple<optional<log_entry_position>, int> engine_merger::insert(const log_entry& entry, shared_ptr<file>& aim_file, shared_ptr<file>& merge_file, shared_ptr<file>& hint_file) {

	auto e = 0;

	auto s = serialize_log_entry(entry);

	if (merge_file->write_offset() + s.size() > _options._capacity)
		return make_tuple(nullopt, error_insufficient_merge_file_space);

	auto offset = merge_file->write_offset();

	if ((e = merge_file->write(s)) < 0)
		return make_tuple(nullopt, e);

	if (_options._synchronize_write)
		if ((e = merge_file->synchronize()) < 0)
			return make_tuple(nullopt, e);

	log_entry_position position(merge_file->file_i(), offset);

	if ((e = hint_file->write(serialize_hint(entry._key, position))) < 0)
		return make_tuple(nullopt, e);

	return make_tuple(make_optional(position), 0);

}






tuple<optional<log_entry_position>, int> engine_merger::insert(const log_entry& entry, shared_ptr<file>& merge_file, shared_ptr<hint_file>& hint_file) {

	auto [_ig, e] = insert_(entry, merge_file, hint_file);

	if (e < 0) {

		if (e == error_insufficient_merge_file_space) {

			++merge_file_i;

			auto [new_merge_file, e_m] = create_merge_file(merge_file_i);

			if (e_m < 0)
				return make_tuple(nullopt, e_m);

			merge_file = new_merge_file.value();

			return merge_insert(entry, merge_file, hint_file);

		}
		else
			return make_tuple(nullopt, e);

	}

	return make_tuple(_ig, 0);
}


tuple<optional<log_entry_position>, int> engine_merger::insert(const log_entry& entry, shared_ptr<file>& _file, shared_ptr<file>& hint_file) {

	auto e = 0;

	auto s = serialize_log_entry(entry);

	if (merge_file->write_offset() + s.size() > _options._capacity)
		return make_tuple(nullopt, error_insufficient_merge_file_space);

	auto offset = merge_file->write_offset();

	if ((e = merge_file->write(s)) < 0)
		return make_tuple(nullopt, e);

	if (_options._synchronize_write)
		if ((e = merge_file->synchronize()) < 0)
			return make_tuple(nullopt, e);

	log_entry_position position(merge_file->file_i(), offset);

	if ((e = hint_file->write(serialize_hint(entry._key, position))) < 0)
		return make_tuple(nullopt, e);

	return make_tuple(make_optional(position), 0);

}


int engine::merge_load() {

	auto [o, e] = _file_manager->open_merge_complete_file();

	if (e < 0)
		return e;

	auto merge_complete_file = o.value();

	auto [merge_information, _, e_a] = merge_complete_file->read(0);

	if (e_a < 0)
		return e_a;

	auto final_merge_file_i = e_serialize_i(merge_information->_value);

	unique_lock<mutex> lock(_mutex);

	for (int i = 0; i <= final_merge_file_i; ++i)
		file_system::remove(_files[i]->path());





	return 0;

}

*/



