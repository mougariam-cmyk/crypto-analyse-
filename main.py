  File "<frozen importlib._bootstrap>", line 1398, in _gcd_import
  File "<frozen importlib._bootstrap>", line 1371, in _find_and_load
  File "<frozen importlib._bootstrap>", line 1342, in _find_and_load_unlocked
  File "<frozen importlib._bootstrap>", line 938, in _load_unlocked
  File "<frozen importlib._bootstrap_external>", line 755, in exec_module
  File "<frozen importlib._bootstrap_external>", line 893, in get_code
  File "<frozen importlib._bootstrap_external>", line 823, in source_to_code
  File "<frozen importlib._bootstrap>", line 491, in _call_with_frames_removed
  File "/opt/render/project/src/main.py", line 85
    reasons_formatted = '\n'.join([f"� {r}" for r in reasons]) if reasons else "� ?? ??? ??? ???????? ?????? ?? ???????? ???????."
                                     ^
SyntaxError: Non-UTF-8 code starting with '\x95' on line 85, but no encoding declared; see https://peps.python.org/pep-0263/ for details
==> Exited with status 1
