#include "engine.h"






































engine_iterator::engine_iterator(const engine& g) :
	_engine(g), _container_iterator(g._container->iterator()) {

}


