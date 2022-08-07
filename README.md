MSKS - reproducible task execution
==================================

MSKS (short for Meeseek) is a lightweight Python tool for running interdependent tasks in a reproducible manner. Reproducible environments are created using Conda, the source code for the task is pinned to a specific version in a given repository.
By design MSKS is somewhat aimed at machine learning experiments, although there is a design guideline to to keep the system quite generic supporting tasks that may not involve model training.

Concepts
--------

 * Task - A combination of a fixed point in a repository, an entry point and arguments to the entrypoint. Each task is identified by a hash identifier based on these parameters. A task can depend on outputs of other tasks
 * Environment - An Conda environment in which a given task is run in. The environment that is specified by the source code descriptor and is created on demand if needed.
 * Repository / commit: Each task needs a Git repository to get source code from and a specific commit ID to specify which version of the code to use.
 * Entrypoint - One or more ways to interact with the code. Entypoints are described using a description file in the source code which specifies what command is actually run, what arguments are accepted and what outputs are produced.

Installation
------------

You can install MSKS using Pip: `pip install msks`. If successful, you can then use MSKS CLI by executing `msks`.

Documentation
-------------

TODO

Acknowledgements
----------------

The development of this package was supported by Sloveninan research agency (ARRS) project Z2-1866.

Licence
-------

Copyright 2022 Luka Čehovin Zajc

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
