#!/usr/bin/env bash
cpplint.py --filter=-build/c++11,-legal/copyright,-build/include_subdir --root=. *.cpp include/*.h
