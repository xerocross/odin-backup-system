from __future__ import annotations
from dataclasses import dataclass
from enum import Enum, auto
from typing import Callable, Dict, Tuple, Optional

# ----- Domain state -----------------------------------------------------------
class S(Enum):
    START          = auto()
    NO_STATE_FILE_OR_NO_OUTPUT = auto()
    
    STATE_FILE_OUTPUT_MATCHES_GENERATED_CURRENT_SIGNATURE = auto()
    
    
    
    CHECK_STATE    = auto()
    CHECK_INPUT    = auto()
    CHECK_OUTPUT   = auto()
    REBUILD        = auto()
    SKIP           = auto()
    ERROR          = auto()
    DONE           = auto()
    
    
    
    

# ----- Context: facts, filled in as we go ------------------------------------
@dataclass
class Ctx:
    # inputs
    manifest_path: str
    state_path: str
    quick_sig: Callable[[], str]       # heavy; compute only when needed
    manifest_hash: Callable[[], str]   # heavy; compute only when needed
    # facts (computed along the way)
    manifest_exists: Optional[bool] = None
    state_exists: Optional[bool] = None
    state_parse_ok: Optional[bool] = None
    input_sig_matches: Optional[bool] = None
    output_hash_matches: Optional[bool] = None
    error: Optional[str] = None

# ----- Actions: compute facts / do work, return next state --------------------
Action = Callable[[Ctx], S]

def start(ctx: Ctx) -> S:
    return S.CHECK_PRESENCE

def check_presence(ctx: Ctx) -> S:
    from pathlib import Path
    mp, sp = Path(ctx.manifest_path), Path(ctx.state_path)
    ctx.manifest_exists = mp.exists()
    ctx.state_exists = sp.exists()
    return S.CHECK_STATE if ctx.state_exists else (S.REBUILD if not ctx.manifest_exists else S.REBUILD)

def check_state(ctx: Ctx) -> S:
    import json, pathlib
    try:
        text = pathlib.Path(ctx.state_path).read_text(encoding="utf-8")
        _ = json.loads(text)  # keep raw; parse again in checks
        ctx.state_parse_ok = True
        return S.CHECK_INPUT
    except Exception as e:
        ctx.state_parse_ok = False
        ctx.error = f"state unreadable: {e}"
        return S.ERROR

def check_input(ctx: Ctx) -> S:
    import json, pathlib
    st = json.loads(pathlib.Path(ctx.state_path).read_text(encoding="utf-8"))
    prev = st.get("initial_signature_hash") or st.get("init_sig_hex")
    cur = ctx.quick_sig()  # heavy; evaluated once here
    ctx.input_sig_matches = (prev == cur)
    return S.CHECK_OUTPUT if ctx.input_sig_matches else S.REBUILD

def check_output(ctx: Ctx) -> S:
    import json, pathlib
    st = json.loads(pathlib.Path(ctx.state_path).read_text(encoding="utf-8"))
    prev = st.get("output_signature_hash") or st.get("output_sig_hex")
    cur = ctx.manifest_hash()
    ctx.output_hash_matches = (prev == cur)
    return S.SKIP if ctx.output_hash_matches else S.REBUILD

def do_rebuild(ctx: Ctx) -> S:
    # your existing write_manifest_and_state(...) goes here
    # if it raises, catch and set ctx.error then go to ERROR
    try:
        # build manifest, write state
        return S.DONE
    except Exception as e:
        ctx.error = str(e)
        return S.ERROR

def do_skip(ctx: Ctx) -> S:
    # finish_run(success) + logging
    return S.DONE

def fail(ctx: Ctx) -> S:
    # finish_run(failed) + logging ctx.error
    return S.DONE

# ----- Transition table -------------------------------------------------------
TRANSITIONS: Dict[S, Action] = {
    S.START:          start,
    S.CHECK_PRESENCE: check_presence,
    S.CHECK_STATE:    check_state,
    S.CHECK_INPUT:    check_input,
    S.CHECK_OUTPUT:   check_output,
    S.REBUILD:        do_rebuild,
    S.SKIP:           do_skip,
    S.ERROR:          fail,
    S.DONE:           lambda ctx: S.DONE,
}

# ----- Engine loop with trace -------------------------------------------------
def run_fsm(ctx: Ctx, *, begin: S = S.START) -> Tuple[S, list[Tuple[S, str]]]:
    trace: list[Tuple[S, str]] = []
    state = begin
    while True:
        action = TRANSITIONS[state]
        trace.append((state, action.__name__))
        next_state = action(ctx)
        if next_state is S.DONE:
            trace.append((S.DONE, "terminal"))
            return next_state, trace
        state = next_state
