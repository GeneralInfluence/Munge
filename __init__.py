#!/usr/bin/env python

# script by dedan: helps him to symlink gensim
import os
dirname = __path__[0]		# Package's main folder
__path__.insert(0, os.path.join(dirname, "Munge"))

__all__ = ['loadCleanly']