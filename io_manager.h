#pragma once

#include "container.h"

#include "log.h"


































#include <ios>

#include <fstream>

#include <filesystem>

namespace file_system = filesystem;


class io_manager {

public:

	enum io_manager_type {

		file_io_type = 0,

	};

	virtual ~io_manager() {
	
	};

	static shared_ptr<io_manager> create_io_manager(io_manager_type, file_system::path);

	virtual tuple<optional<vector<char>>, int> read(int64_t, int64_t) = 0;

	virtual int write(const vector<char>& v) = 0;

	virtual int synchronize() = 0;

	virtual int close() = 0;

	virtual uint64_t size() = 0;

	virtual void print()const = 0;

	file_system::path path()const {

		return _path;

	}

protected:

	file_system::path _path;

	io_manager(file_system::path);

};


class file_io :public io_manager
{

public:

	file_io(file_system::path);

	virtual tuple<optional<vector<char>>, int> read(int64_t, int64_t);

	virtual int write(const vector<char>& v);

	virtual int synchronize();

	virtual int close();

	virtual uint64_t size();

	virtual void print()const;

private:

	fstream _file;

};


