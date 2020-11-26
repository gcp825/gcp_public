#!/usr/bin/python3

#######################################################################################################
# translate:
#  - A direct equivalent of the standard SQL translate function (inc. the illogical ordering of the
#    the to (repl) and from (find) strings).
#  - Far less clunky than the out-of-the-box python translate implementation.
#######################################################################################################

def translate(x,repl,find,pad=' '):
    
    repl = repl[0:len(find)]
    diff = len(find) - len(repl)
 
    pairs = zip([char for char in find], [char for char in repl] + ([pad] * diff))

    for a,b in pairs: x = x.replace(a,b)
    
    return x
