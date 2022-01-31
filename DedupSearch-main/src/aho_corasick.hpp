/*
* Copyright (C) 2020 Christopher Gilbert and Amnon Hanuhov.
* Based on https://github.com/cjgdev/aho_corasick
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
*/

#ifndef AHO_CORASICK_HPP
#define AHO_CORASICK_HPP
#include <algorithm>
#include <cctype>
#include <map>
#include <memory>
#include <set>
#include <list>
#include <string>
#include <queue>
#include <utility>
#include <vector>
#include <unordered_map>

namespace aho_corasick {
    typedef enum match_type{ FULL, PREFIX, SUFFIX } match_type;
	// class state
	template<typename CharType>
	class state {
	public:
		typedef state<CharType>*                                     ptr;
		typedef std::unique_ptr<state<CharType>>                     unique_ptr;
		typedef std::basic_string<CharType>                          string_type;
		typedef std::basic_string<CharType>&                         string_ref_type;
		typedef std::pair<unsigned, std::pair<unsigned, bool>> 	     key_index;
		typedef std::set<key_index>                                  index_collection;
		typedef std::vector<ptr>                                     state_collection;
		typedef std::vector<CharType>                                transition_collection;

	private:
		size_t                         d_depth;
		ptr                            d_root;
		std::vector<unique_ptr>        d_success;
		ptr                            d_failure;
		bool						   d_has_keyword;
		index_collection               d_emits;

	public:
		state(): state(0) {}

		explicit state(size_t depth)
			: d_depth(depth)
			, d_root(depth == 0 ? this : nullptr)
			, d_success(256)
			, d_failure(nullptr)
			, d_has_keyword(false)
			, d_emits() {}

		ptr next_state(CharType character) const {
			return next_state(character, false);
		}

		ptr next_state_ignore_root_state(CharType character) const {
			return next_state(character, true);
		}

		ptr add_state(CharType character) {
			auto next = next_state_ignore_root_state(character);
			if (next == nullptr) {
				next = new state<CharType>(d_depth + 1);
				d_success[character].reset(next);
			}
			return next;
		}

		void set_has_keyword() { d_has_keyword = true; }

		bool has_keyword() { return d_has_keyword; }

		size_t get_depth() const { return d_depth; }

		void add_emit(unsigned keyword_index, unsigned position, bool is_keyword) {
		    std::pair<unsigned, bool> pos_and_is_keyword = std::make_pair(position, is_keyword);
			d_emits.insert(std::make_pair(keyword_index, pos_and_is_keyword));
		}

		void add_emit(const index_collection& emits) {
			for (const auto& e : emits) {
				add_emit(e.first, e.second.first, e.second.second);
			}
		}

		index_collection get_emits() const { return d_emits; }

		ptr failure() const { return d_failure; }

		void set_failure(ptr fail_state) { d_failure = fail_state; }

		state_collection get_states() const {
			state_collection result;
			for (auto it = d_success.cbegin(); it != d_success.cend(); ++it) {
			    if (it->get())
				    result.push_back(it->get());
			}
			return state_collection(result);
		}

		transition_collection get_transitions() const {
			transition_collection result;
			for (int i = 0; i < d_success.size(); i++) {
			    if (d_success[i])
				    result.push_back(i);
			}
			return transition_collection(result);
		}

	private:
		ptr next_state(CharType character, bool ignore_root_state) const {
			ptr result = nullptr;
			if (character < 0 || character >= d_success.size())
				return d_root;
			if (d_success[character]) {
				result = d_success[character].get();
			} else if (!ignore_root_state && d_root != nullptr) {
				result = d_root;
			}
			return result;
		}
	};

	template<typename CharType>
	class basic_trie {
	public:
		using string_type = std::basic_string < CharType > ;
		using string_ref_type = std::basic_string<CharType>&;
		

		typedef state<CharType>         				state_type;
		typedef state<CharType>*        				state_ptr_type;
		typedef std::unordered_map<unsigned, std::unordered_map<match_type, std::pair<unsigned, std::vector<unsigned>>>> emit_collection; 

		class config {
			bool d_case_insensitive;

		public:
			config()
				: d_case_insensitive(false) {}

			bool is_case_insensitive() const { return d_case_insensitive; }
			void set_case_insensitive(bool val) { d_case_insensitive = val; }
		};

	private:
		std::unique_ptr<state_type> d_root;
		config                      d_config;
		bool                        d_constructed_failure_states;
		unsigned                    d_num_keywords = 0;
		unsigned                    d_longest_keyword = 0;

	public:
		basic_trie(): basic_trie(config()) {}

		basic_trie(const config& c)
			: d_root(new state_type())
			, d_config(c)
			, d_constructed_failure_states(false) {}

		basic_trie& case_insensitive() {
			d_config.set_case_insensitive(true);
			return (*this);
		}
		
		void check_construct_failure_states() {
			if (!d_constructed_failure_states) {
				construct_failure_states();
			}
		}

		void insert(string_type keyword, unsigned keyword_index, bool reverse) {
			if (keyword.empty())
				return;
			state_ptr_type cur_state = d_root.get();
			unsigned keyword_length = keyword.length();
			if (keyword_length > d_longest_keyword)
			    d_longest_keyword = keyword_length;
			for (int i = 0; i < keyword_length; i++) {
			    cur_state = cur_state->add_state(keyword[i]);
			    if (i == keyword_length-1) {
			        cur_state->set_has_keyword();
			        cur_state->add_emit(keyword_index, i+1, true);
			    }
			    else
			        cur_state->add_emit(keyword_index, i+1, false);
			    
			}
			d_constructed_failure_states = false;
		}

		emit_collection parse_text(string_type& text, bool stop_at_full, emit_collection& collected_emits) {
			size_t pos = 0;
			state_ptr_type cur_state = d_root.get();
			unsigned text_length = text.size();
			for (int i = 0; i < text_length; i++) {
				cur_state = get_state(cur_state, text[i]);
				if (cur_state->has_keyword()) {
				    store_emits(pos, cur_state, collected_emits, stop_at_full, FULL);
				}
				pos++;
			}
			store_emits(text_length-1, cur_state, collected_emits, stop_at_full, PREFIX);
			return collected_emits;
		}

		emit_collection parse_reverse_text(string_type& text, bool stop_at_full, emit_collection& collected_emits) {
			state_ptr_type cur_state = d_root.get();
			unsigned text_length = text.size();
			if (text_length > d_longest_keyword)
			    text_length = d_longest_keyword;
			for (int i = text_length-1; i >= 0; --i) {
				// if (d_config.is_case_insensitive()) {
				// 	text[i] = std::tolower(text[i]);
				// }
				cur_state = get_state(cur_state, text[i]);
			}
			store_emits(-1, cur_state, collected_emits, stop_at_full, SUFFIX);
			return collected_emits;
		}

	private:
		state_ptr_type get_state(state_ptr_type cur_state, CharType c) const {
			state_ptr_type result = cur_state->next_state(c);
			while (result == nullptr) {
				cur_state = cur_state->failure();
				result = cur_state->next_state(c);
			}
			return result;
		}

		void construct_failure_states() {
			std::queue<state_ptr_type> q;
			for (auto& depth_one_state : d_root->get_states()) {
				depth_one_state->set_failure(d_root.get());
				q.push(depth_one_state);
			}
			d_constructed_failure_states = true;

			while (!q.empty()) {
				auto cur_state = q.front();
				for (const auto& transition : cur_state->get_transitions()) {
					state_ptr_type target_state = cur_state->next_state(transition);
					q.push(target_state);

					state_ptr_type trace_failure_state = cur_state->failure();
					while (trace_failure_state->next_state(transition) == nullptr) {
						trace_failure_state = trace_failure_state->failure();
					}
					state_ptr_type new_failure_state = trace_failure_state->next_state(transition);
					target_state->set_failure(new_failure_state);
					target_state->add_emit(new_failure_state->get_emits());
				}
				q.pop();
			}
		}

		void store_emits(int pos, state_ptr_type& cur_state, emit_collection& collected_emits, bool stop_at_full, match_type mtype) const {
			auto emits = cur_state->get_emits();
			if (!emits.empty()) {
				for (const auto& emit : emits) {
				    if (emit.second.second && mtype == FULL) {
				        // This is a full keyword match
				        if (collected_emits[emit.first].find(FULL) == collected_emits[emit.first].end()) {
				            std::vector<unsigned> match_pos;
				            match_pos.push_back(pos - emit.second.first + 1);
				            std::pair<unsigned, std::vector<unsigned>> length_and_pos = std::make_pair(emit.second.first, match_pos);
				            collected_emits[emit.first].insert(std::make_pair(mtype, length_and_pos));
				        }
				        else
				            collected_emits[emit.first][mtype].second.push_back(pos - emit.second.first + 1);
				    }
					else if (!emit.second.second && mtype != FULL && !stop_at_full) {
					    std::vector<unsigned> match_pos;
					    if (mtype == PREFIX)
					        match_pos.push_back(pos - emit.second.first + 1);
					    else
					        match_pos.push_back(0);
					    std::pair<unsigned, std::vector<unsigned>> length_and_pos = std::make_pair(emit.second.first, match_pos);
				        collected_emits[emit.first][mtype] = length_and_pos;
					}
				}
			}
		}
	};

	typedef basic_trie<unsigned char>	trie;
	typedef basic_trie<wchar_t>  		wtrie;


} // namespace aho_corasick

#endif // AHO_CORASICK_HPP


