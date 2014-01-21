// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include <errno.h>
#include <algorithm>

#include "common/debug.h"
#include "osd/ErasureCodePlugin.h"
#include "json_spirit/json_spirit_writer.h"

#include "ErasureCodePyramid.h"

// re-include our assert to clobber boost's
#include "include/assert.h" 

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodePyramid: ";
}

  /*
               
  take root
  set choose datacenter 3
  set choose osd 7

   000000^111111^222222^ datacenter
   0000^^-0000^^-0000^^- 

[
    { "plugin": "xor",
      "k": 6,
      "m": 1,
      "buckets": "datacenter",
      "mapping": "000000^111111^222222^",
    },

    { "plugin": "jerasure",
      "technique": "cauchy_good",
      "k": 12,
      "m": 6,
      "mapping": "0000^^-0000^^-0000^^-",
    },
]

  take root
  set choose datacenter 2
  set choose osd 3

   -^000111^-  datacenter
   ^-000111-^

[
    { "plugin": "xor",
      "k": 3,
      "m": 1,
      "buckets": "datacenter",
      "mapping": "-^000111^-",
    },

    { "plugin": "jerasure",
      "technique": "cauchy_good",
      "k": 6,
      "m": 2,
      "mapping": "^-000111-^",
    },
]


  take root
  set choose datacenter 2
  set choose rack 2
  set choose osd 3

   --000^111^^222^333--  rack
   -^000-000--111-111^-  datacenter
   ^-000-000--000-000-^  

[
    { "erasure-code-plugin": "xor",
      "erasure-code-k": 3,
      "erasure-code-m": 1,
      "type": "rack",
      "size": 2,
      "mapping": "--000^111^^222^333--",
    },

    { "plugin": "xor",
      "k": 6,
      "m": 1,
      "type": "datacenter",
      "size": 2,
      "mapping": "-^000-000--111-111^-",
    },

    { "plugin": "jerasure",
      "technique": "cauchy_good",
      "k": 12,
      "m": 2,
      "mapping": "^-000-000--000-000-^",
    },
]

  */

int ErasureCodePyramid::layers_description(const map<std::string,std::string> &parameters,
					   json_spirit::mArray *description,
					   ostream *ss) const
{
  if (parameters.count("erasure-code-pyramid") == 0) {
    *ss << "could not find 'erasure-code-pyramid' in " << parameters;
    return ERROR_PYRAMID_DESCRIPTION;
  }
  string str = parameters.find("erasure-code-pyramid")->second;
  try {
    json_spirit::mValue json;
    json_spirit::read_or_throw(str, json);

    if (json.type() != json_spirit::array_type) {
      *ss << "erasure-code-pyramid='" << str
	 << "' must be a JSON array but is of type "
	 << json.type() << " instead";
      return ERROR_PYRAMID_ARRAY;
    }
    *description = json.get_array();
  } catch (json_spirit::Error_position &e) {
    *ss << "failed to parse erasure-code-pyramid='" << str << "'"
       << " at line " << e.line_ << ", column " << e.column_
       << " : " << e.reason_;
    return ERROR_PYRAMID_PARSE_JSON;
  }
  return 0;
}

int ErasureCodePyramid::layers_parse(string description_string,
				     json_spirit::mArray description,
				     ostream *ss)
{
  ErasureCodePluginRegistry &registry = ErasureCodePluginRegistry::instance();

  int position = 0;
  for (vector<json_spirit::mValue>::iterator i = description.begin();
       i != description.end();
       ++i, ++position) {
    if (i->type() != json_spirit::obj_type) {
      stringstream json_string;
      json_spirit::write(*i, json_string);
      *ss << "each element of the array "
	  << description_string << " must be a JSON object but "
	  << json_string.str() << " at position " << position
	  << " is of type " << i->type() << " instead";
      return ERROR_PYRAMID_TYPE;
    }
    json_spirit::mObject layer_json = i->get_obj();
    Layer layer;
    for (map<string, json_spirit::mValue>::iterator j = layer_json.begin();
	 j != layer_json.end();
	 ++j) {
      if (j->second.type() != json_spirit::str_type) {
	stringstream json_string;
	json_spirit::write(*i, json_string);
	*ss << "element " << j->first << " from object "
	    << json_string.str() << " at position " << position
	    << " is of type " << j->second.type() << " instead";
	return ERROR_PYRAMID_STR;
      }
      layer.parameters[j->first] = j->second.get_str();
    }
    if (layer.parameters.count("erasure-code-plugin") == 0) {
      stringstream json_string;
      json_spirit::write(*i, json_string);
      *ss << "missing \"erasure-code-plugin\" entry in " 
	  << json_string.str() << " at position " << position;
      return ERROR_PYRAMID_PLUGIN;
    }
    layer.parameters["erasure-code-directory"] = directory;
    int r = registry.factory(layer.parameters["erasure-code-plugin"],
			     layer.parameters,
			     &layer.erasure_code);
    if (r)
      return r;
    layers.push_back(layer);
  }

  return 0;
}

int ErasureCodePyramid::layers_init(string description_string, ostream *ss)
{
  data_chunk_count = layers.back().erasure_code->get_data_chunk_count();
  chunk_count = layers.back().parameters["mapping"].size();
  for (list<Layer>::iterator layer = layers.begin();
       layer != layers.end();
       layer++) {
    string &mapping = layer->parameters["mapping"];
    layer->mapping = mapping.c_str();
    layer->mapping_length = mapping.size();
    if (layer->parameters.count("size")) {
      std::string err;
      int size = strict_strtol(layer->parameters["size"].c_str(), 10, &err);
      if (!err.empty()) {
	*ss << "could not convert size because " << err;
	return ERROR_PYRAMID_INT;
      }
      layer->size = size;
    } else {
      layer->size = 1;
    }
  }
  return 0;
}

int ErasureCodePyramid::layers_sanity_checks(string description_string,
					     ostream *ss) const
{
  if (layers.size() < 2) {
    *ss << "at least two layers must be defined, " << layers.size()
       << " were found in " << description_string;
    return ERROR_PYRAMID_TWO_LAYERS;
  }

  {
    const string &first_mapping =
      layers.front().parameters.find("mapping")->second;
    int level = 1;
    for (list<Layer>::const_iterator layer = layers.begin();
	 layer != layers.end();
	 layer++, level++) {
      const string &mapping = layer->parameters.find("mapping")->second;
      if (first_mapping.size() != mapping.size()) {
	*ss << "layer " << level << " has mapping='" << mapping << "'"
	   << " which must be of the same size as the mapping of the "
	   << " first layer ('" << first_mapping << "')";
	return ERROR_PYRAMID_MAPPING_SIZE;
      }
    }
  }

  {   
    list<Layer>::const_iterator lower = layers.begin();
    list<Layer>::const_iterator upper = layers.begin();
    int level = 0;
    list<string> fields;
    fields.push_back("mapping");
    fields.push_back("size");
    fields.push_back("type");
    for (++upper; upper != layers.end(); ++upper, ++lower, ++level) {
      for (list<string>::iterator field = fields.begin();
	   field != fields.end();
	   field++) {
	if (lower->parameters.count(*field) == 0) {
	  *ss << "layer " << level << " in " << description_string
	      << " is missing the mandatory field " << *field;
	  return ERROR_PYRAMID_MISSING_FIELD;
	}
      }
      if (chunk_count % lower->erasure_code->get_chunk_count()) {
	*ss << "layer " << level << " has "
	   << lower->erasure_code->get_chunk_count() << " chunks"
	   << " which is not a multiple of " << chunk_count << ","
	   << " the total number of chunks all layers included";
	return ERROR_PYRAMID_COUNT_CONSTRAINT;
      }
    }
  }

  return 0;
}

int ErasureCodePyramid::init(const map<std::string,std::string> &parameters,
			     ostream *ss)
{
  if (parameters.count("erasure-code-directory") != 0)
    directory = parameters.find("erasure-code-directory")->second;

  int r;

  json_spirit::mArray description;
  r = layers_description(parameters, &description, ss);
  if (r)
    return r;

  string description_string = parameters.find("erasure-code-pyramid")->second;

  dout(10) << "init(" << description_string << ")" << dendl;

  r = layers_parse(description_string, description, ss);
  if (r)
    return r;

  r = layers_init(description_string, ss);
  if (r)
    return r;

  return layers_sanity_checks(description_string, ss);
}

#if 0
int ErasureCodePyramid::add_crush_step(json_spirit::mValue json,
				       ostream *ss)
{
  if (json.type() != json_spirit::array_type) {
    ss << str << " must be a JSON array but is of type "
       << json.type() << " instead";
    return -EINVAL;
  }

  vector<json_spirit::mValue>::iterator i = json.begin();
  if (i->type() != json_spirit::int_type)
    return EINVAL;
  int count = i->get_int();

  ++i;
  if (i->type() != json_spirit::str_type)
    return EINVAL;
  int type = i->get_str();

  crush_steps[type] = count;

  return 0;
}
#endif

unsigned int ErasureCodePyramid::get_chunk_size(unsigned int object_size) const
{
  list<Layer>::const_reverse_iterator i = layers.rbegin();
  unsigned int chunk_size = i->erasure_code->get_chunk_size(object_size);
  unsigned int padded_size = data_chunk_count * chunk_size;
  for (++i; i != layers.rend(); i++) {
    assert(padded_size % i->size == 0);
    padded_size /= i->size;
    assert(chunk_size == i->erasure_code->get_chunk_size(padded_size));
  }
  return chunk_size;
}

int ErasureCodePyramid::minimum_to_decode(const set<int> &want_to_read,
                                           const set<int> &available_chunks,
                                           set<int> *minimum) 
{
  if (includes(available_chunks.begin(), available_chunks.end(),
	       want_to_read.begin(), want_to_read.end())) {
    *minimum = want_to_read;
  } else {
#if 0
    if (available_chunks.size() < (unsigned)k)
      return -EIO;
    set<int>::iterator i;
    unsigned j;
    for (i = available_chunks.begin(), j = 0; j < (unsigned)k; ++i, j++)
      minimum->insert(*i);
#endif
  }
  return 0;
}

int ErasureCodePyramid::minimum_to_decode_with_cost(const set<int> &want_to_read,
                                                     const map<int, int> &available,
                                                     set<int> *minimum)
{
  set <int> available_chunks;
  for (map<int, int>::const_iterator i = available.begin();
       i != available.end();
       ++i)
    available_chunks.insert(i->first);
  return minimum_to_decode(want_to_read, available_chunks, minimum);
}

int ErasureCodePyramid::layer_encode(Layer layer,
				     list<bufferlist> &chunks)
{
  // Call the encoding function by remapping data + coding chunks so
  // that coding chunks follow data chunks without gaps in between.
  // All chunks are pre-allocated and the coding function is expected
  // to use them and the re-ordering of the chunks involves exchanging
  // pointers and not copying data

  const char *mapping = layer.mapping.c_str();
  unsigned int mapping_length = layer.mapping.size();
  list<bufferlist> data;
  list<bufferlist> coding;
  for (unsigned int i = 0; i < mapping_length; i++) {
    switch (mapping[i]) {
    case '^':
      data.push_back(chunks[i]);
      break;
    case '-':
      break;
    default:
      coding.push_back(chunks[i]);
      break;
    } 
  }
  list<bufferlist> layer_chunks = data + coding;
  int r = layer.erasure_code->encode(layer_chunks);
  if (r)
    return r;

  for (list<Layer>::iterator layer = layer.layers.begin();
       layer != layer.layers.end();
       ++layer) {
    list<bufferlist> local_chunks;
    for (unsigned int j = 0; j < layer->mapping.size(); j++) {
      local_chunks.push_back(chunks.front());
      chunks.pop_front();
    }
    r = layer_encode(layer, local_chunks);
    if (r)
      return r;
  }

  return 0;
}

int ErasureCodePyramid::encode(list<bufferlist> &chunks)
{
  return layer_encode(layers, layers.back()->mapping, chunks);
}

int ErasureCodePyramid::layer_decode(Layer &layer,
				     list<bool> *erasures,
				     list<bufferlist> &chunks)
{
  list<bufferlist> global_chunks = chunks;
  list<bool> global_erasures = *erasures;
  erasures.clear();
  for (list<Layer>::iterator layer = layer.layers.begin();
       layer != layer.layers.end();
       ++layer) {
    list<bufferlist> local_chunks;
    list<bool> local_erasures;
    for (unsigned int j = 0; j < layer->mapping.size(); j++) {
      local_chunks.push_back(global_chunks.front());
      global_chunks.pop_front();
      local_erasures.push_back(global_erasures->front());
      global_erasures.pop_front();
    }
    r = layer_decode(layer, &local_erasures, local_chunks);
    if (r && r != -EIO)
      return r;
    *erasures += local_erasures;
  }

  
  const char *mapping = layer.mapping.c_str();
  unsigned int mapping_length = layer.mapping.size();
  list<bufferlist> data;
  list<bufferlist> coding;
  unsigned int chunk_index;
  for (unsigned int i = 0; i < mapping_length; i++) {
    switch (mapping[i]) {
    case '^':
      data.push_back(chunks[i]);
      layer_erasures.push_back((*erasures)[i]);
      break;
    case '-':
      break;
    default:
      coding.push_back(chunks[i]);
      layer_erasures.push_back((*erasures)[i]);
      break;
    } 
  }
  list<bufferlist> layer_chunks = data + coding;
  int r = layer.erasure_code->encode(layer_erasures, layer_chunks);
  if (r)
    return r;
  erasures->clear();
  for (unsigned int i = 0; i < mapping_length; i++) {
    if (mapping[i] != '-') {
      erasures[i] = layer_erasures.front();
      layer_erasures.pop_front();
    }
  }

  return 0;
}

int ErasureCodePyramid::decode(const set<int> &want_to_read,
                                const map<int, bufferlist> &chunks,
                                map<int, bufferlist> *decoded)
{
  // TODO: to save some computation, if all chunks are available,
  // don't bother to iterate from local to global, just decode at the
  // global level

  unsigned blocksize = chunks.begin()->second.length();
  unsigned int chunk_count = get_chunk_count();
  for (unsigned int i = 0; i < chunk_count; i++) {
    if (chunks.count(i) == 0) {
      bufferptr ptr(buffer::create_page_aligned(blocksize));
      (*decoded)[i].push_front(ptr);
    } else {
      (*decoded)[i] = chunks.find(i)->second;
    }
  }

  set<int> left_to_read = want_to_read;
  for (list<Layer>::iterator layer = layers.begin();
       layer != layers.end() && !left_to_read.empty();
       layer++) {
    bool do_decode = false;
    unsigned int chunk_count = layer->erasure_code->get_chunk_count();
    const char *mapping = layer->mapping;
    unsigned int mapping_length = layer->mapping_length;
    unsigned int i = 0;
    do {
      bufferlist local_data;
      map<int, bufferlist> local_chunks;
      map<int, bufferlist> local_decoded;
      set<int> local_want_to_read;
      set<int> global_want_to_read;
      set<int> available;
      unsigned int chunk_index = 0;
      for ( ; chunk_index < chunk_count && i < mapping_length ; i++) {
	if (mapping[i] != '-') {
	  if (chunks.count(i) != 0) {
	    local_chunks[chunk_index] = chunks.find(i)->second;
	    available.insert(chunk_index);
	  }
	  local_decoded[chunk_index] = decoded->find(i)->second;
	}

	if (want_to_read.count(i) != 0) {
	  do_decode = true;
	  local_want_to_read.insert(chunk_index);
	  global_want_to_read.insert(i);
	}
	chunk_index++;
      }

      if (do_decode) {
	map<int, bufferlist> decoded;
	set<int> minimum;
	int r;
	r = layer->erasure_code->minimum_to_decode(local_want_to_read,
						   available,
						   &minimum);
	if (r && r != -EIO)
	  return r;

	// silently ignore when there are not enough chunks to repair,
	// hoping it will be resolved in an upper layer
	if (r != -EIO) {
	  r = layer->erasure_code->decode(local_want_to_read,
					  local_chunks,
					  &local_decoded);
	  if (r)
	    return r;
	
	  for (set<int>::iterator i = global_want_to_read.begin();
	       i != global_want_to_read.end();
	       i++)
	    left_to_read.erase(*i);
	}
      }
    } while(i < mapping_length);
  }

  if (!left_to_read.empty())
    return -EIO;
  else
    return 0;
}


