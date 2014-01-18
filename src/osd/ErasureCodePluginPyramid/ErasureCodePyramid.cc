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
#include "json_spirit/json_spirit_utils.h"

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
  if (parameters.find("erasure-code-pyramid") == parameters.end()) {
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
	 ++j)
      layer.parameters[j->first] = j->second.get_str();
    if (layer.parameters.find("erasure-code-plugin") == layer.parameters.end()) {
      stringstream json_string;
      json_spirit::write(*i, json_string);
      *ss << "missing \"erasure-code-plugin\" entry in " 
	  << json_string.str() << " at position " << position;
      return ERROR_PYRAMID_PLUGIN;
    }
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
  data_chunk_count = layers.front().erasure_code->get_data_chunk_count();
  chunk_count = layers.front().parameters["mapping"].size();
  for (vector<Layer>::iterator layer = layers.begin();
       layer != layers.end();
       layer++) {
    string &mapping = layer->parameters["mapping"];
    layer->mapping = mapping.c_str();
    layer->mapping_length = mapping.size();
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
    for (vector<Layer>::const_iterator layer = layers.begin();
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
    vector<Layer>::const_iterator lower = layers.begin();
    vector<Layer>::const_iterator upper = layers.begin();
    int level = 0;
    list<string> fields;
    fields.push_back("mapping");
    fields.push_back("size");
    fields.push_back("type");
    for (++upper; upper != layers.end(); ++upper, ++lower, ++level) {
      for (list<string>::iterator field = fields.begin();
	   field != fields.end();
	   field++) {
	if (lower->parameters.find(*field) == lower->parameters.end()) {
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
  vector<Layer>::const_iterator i = layers.begin();
  unsigned int chunk_size = i->erasure_code->get_chunk_size(object_size);
  for (++i; i != layers.end(); i++)
    assert(chunk_size == i->erasure_code->get_chunk_size(object_size));
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

int ErasureCodePyramid::encode(const set<int> &want_to_encode,
			       const bufferlist &in,
			       map<int, bufferlist> *encoded)
{
  unsigned blocksize = get_chunk_size(in.length());
  unsigned k = get_data_chunk_count();
  unsigned padded_length = blocksize * k;
  assert(padded_length == in.length());
  bufferlist out(in);
  unsigned m = get_chunk_count() - get_data_chunk_count();
  unsigned coding_length = blocksize * m;
  bufferptr coding(buffer::create_page_aligned(coding_length));
  out.push_back(coding);
  for (unsigned int i = 0; i < k + m; i++) {
    bufferlist &chunk = (*encoded)[i];
    chunk.substr_of(out, i * blocksize, blocksize);
  }

  for (vector<Layer>::iterator layer = layers.begin();
       layer != layers.end();
       layer++) {
    unsigned int chunk_count = layer->erasure_code->get_chunk_count();
    const char *mapping = layer->mapping;
    unsigned int mapping_length = layer->mapping_length;
    unsigned int i = 0;
    do {
      bufferlist local_data;
      map<int, bufferlist> local_encoded;
      set<int> local_want_to_encode;
      unsigned int chunk_index = 0;
      unsigned int encoded_index = 0;
      for ( ; chunk_index < chunk_count && i < mapping_length ; i++) {
	if (mapping[i] != '-') {
	  if (mapping[i] == '^')
	    local_encoded[encoded_index++] = (*encoded)[i];
	  else
	    local_data.append((*encoded)[i]);
	  if (want_to_encode.find(i) != want_to_encode.end())
	    local_want_to_encode.insert(chunk_index);
	  chunk_index++;
	}
      }
      assert(chunk_index == chunk_count);
      int r = layer->erasure_code->encode(local_want_to_encode, local_data, &local_encoded);
      if (r)
	return r;
    } while(i < mapping_length);
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

  unsigned blocksize = (*chunks.begin()).second.length();
  unsigned int chunk_count = get_chunk_count();
  for (unsigned int i = 0; i < chunk_count; chunk_count++) {
    if (chunks.find(i) == chunks.end()) {
      bufferptr ptr(blocksize);
      (*decoded)[i].push_front(ptr);
    } else {
      (*decoded)[i] = chunks.find(i)->second;
    }
  }

  set<int> left_to_read = want_to_read;
  for (vector<Layer>::iterator layer = layers.begin();
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
      set<int> available;
      unsigned int chunk_index = 0;
      for ( ; chunk_index < chunk_count && i < mapping_length ; i++) {
	if (mapping[i] != '-') {
	  if (chunks.find(i) != chunks.end()) {
	    local_chunks[chunk_index] = chunks.find(i)->second;
	    available.insert(chunk_index);
	  }
	  local_decoded[chunk_index] = decoded->find(i)->second;
	}

	if (want_to_read.find(i) != want_to_read.end()) {
	  do_decode = true;
	  local_want_to_read.insert(chunk_index);
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
	
	  for (set<int>::iterator i = local_want_to_read.begin();
	       i != local_want_to_read.end();
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


