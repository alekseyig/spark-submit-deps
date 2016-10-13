import sys
import importlib

if len(sys.argv) == 1:
    raise SyntaxError("Please provide a module to load.")
module = importlib.import_module(sys.argv[1])
sys.exit(module.main(sys.argv[2:]))
