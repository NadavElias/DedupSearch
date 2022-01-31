/*
* Copyright (C) 2020 Amnon Hanuhov.
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

#include "aho_corasick.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <list>

namespace ac = aho_corasick;
using trie = ac::trie;

typedef std::basic_string<unsigned char> u_string;

struct tries{	
        aho_corasick::trie t;	
        aho_corasick::trie tr;	
};

std::vector<std::vector<std::vector<std::vector<unsigned>>>> keywords_ps_matches;

/* 
Inputs:
        - std::vector<std::string> blocks - A vector containing all the blocks (strings)

        - bool stop_at_full - A flag indicating whether we should stop searching after finding a full match of a keyword inside a block

        - trie& t - An Aho-Corasick automaton containing the keywords

        - trie& tr - An Aho-Corasick automaton containing the keywords after reversing them
Output:
        Returns a vector of match collections per block
**/
std::vector<trie::emit_collection> search_block(std::vector<u_string>& blocks, bool stop_at_full, struct tries* tries) {
        std::vector<trie::emit_collection> matches;
        for (auto& block : blocks) {
                trie::emit_collection block_matches;
                tries->t.parse_text(block, stop_at_full, block_matches);
                if (!stop_at_full)
                    tries->tr.parse_reverse_text(block, stop_at_full, block_matches);
                matches.push_back(block_matches);
        }
        return matches;
}

/* 
Inputs:
        - std::pair<unsigned, std::pair<unsigned, unsigned>> keyword_prefix_suffix - Keyword index and a pair of prefix length and suffix length
Output:
        Returns a vector of positions of full matches inside the block: str(prefix+suffix)
**/
std::vector<unsigned> search_concatenation(unsigned keyword_index, unsigned keyword_prefix_length, unsigned keyword_suffix_length) {
        // unsigned keyword_index = keyword_prefix_suffix.first;
        // unsigned keyword_prefix_length = keyword_prefix_suffix.second.first;
        // unsigned keyword_suffix_length = keyword_prefix_suffix.second.second;
        return keywords_ps_matches[keyword_index][keyword_prefix_length][keyword_suffix_length];
}

/* 
Inputs:
        - std::vector<std::string> keywords - The collection of keywords
Output:
        Returns a vector where each index correlates to one keyword and contains a 2D vector where each row correlates to prefix
        length and each column correlates to a suffix length. The vector at position [k][i][j] contains a vector of offsets where there is a full keyword match for the concatenation
        of prefix of length i and suffix of length j of keyword k
**/
std::vector<std::vector<std::vector<std::vector<unsigned>>>> keyword_matches(std::vector<u_string> keywords) {
        std::vector<std::vector<std::vector<std::vector<unsigned>>>> ps_matches;
        std::vector<trie::emit_collection> matches;
        std::vector<u_string> blocks;
        unsigned keyword_index = 0;
        for (const auto& keyword : keywords) {
                struct tries tries;
                tries.t.insert(keyword, keyword_index, false);
                tries.t.check_construct_failure_states();
                tries.tr.check_construct_failure_states();
                unsigned keyword_length = keyword.length();
                std::vector<std::vector<std::vector<unsigned>>> prefixes(keyword_length, std::vector<std::vector<unsigned>>(keyword_length, std::vector<unsigned>{}));
                // Iterate over all prefixes
                for (unsigned i = 0; i < keyword_length-1; i++) {
                        // Iterate over all suffixes
                        for (unsigned j = 1; j < keyword_length; j++) {
                                // Create a block out of the concatenation of the prefix and the suffix
                                blocks.push_back(keyword.substr(0, i+1) + keyword.substr(j, keyword_length - j));
                        }

                        matches = search_block(blocks, true, &tries);
                        std::set<unsigned> suffixes;
                        int k = 1;
                        for (const auto& block_matches : matches) {
                                // Insert the length of a suffix creating a full keyword match with the prefix of length i+1
                                const auto& block_keyword_match_types = block_matches.find(keyword_index);
                                if (block_keyword_match_types != block_matches.end()) {
                                    const auto& match = block_keyword_match_types->second.find(ac::FULL);
                                    if (match != block_keyword_match_types->second.end())
                                            prefixes[i+1][keyword_length - k] = match->second.second;
                                }
                                k++;
                        }
                        blocks.clear();
                        matches.clear();
                }
                ps_matches.push_back(prefixes);
                keyword_index++;
        }
        return ps_matches;
}

/* 
Inputs:
        - std::vector<std::string> keywords - The collection of keywords

        - trie& t - An Aho-Corasick automaton containing the keywords

        - trie& tr - An Aho-Corasick automaton containing the keywords after reversing them
Output:
        Returns a vector of match collections per block
**/
void init(std::vector<u_string> keywords, struct tries* tries) {
    unsigned i = 0;
    for (auto keyword : keywords) {
        tries->t.insert(keyword, i, false);
        reverse(keyword.begin(), keyword.end());
        tries->tr.insert(keyword, i, true);
        i++;
    }
    tries->t.check_construct_failure_states();
    tries->tr.check_construct_failure_states();
    keywords_ps_matches = keyword_matches(keywords);
}