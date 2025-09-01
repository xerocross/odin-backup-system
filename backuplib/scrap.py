@action(DecisionSignal.LOAD_PREVIOUS_OUTPUT)
def _load_prev_output(ctx: Context) -> None:
    ctx.previous_outfile_exists = _exists(ctx.manifest_path)
    ctx.output_loaded = True

@action(DecisionSignal.LOAD_PREVIOUS_RUN_STATE)
def _load_prev_state_presence(ctx: Context) -> None:
    ctx.state_loaded = True
    # presence only; parsing is separate
    # (keep presence/parse split to make failures explainable)



def _exists(p: Path) -> bool:
    try:
        return p.exists()
    except Exception:
        return False
    


@dataclass(frozen=True)
class Signals:
    output_exists: bool
    output_is_valid: bool
    statefile_exists: bool
    state_parse_ok: bool
    statefile_input_signature_hash : str
    statefile_output_signature_hash : str
    current_computed_signature_hash : str



@action(DecisionSignal.PARSE_PREVIOUS_STATE)
def _parse_state(ctx: Context) -> None:
    try:
        if not _exists(ctx.state_path):
            raise FileNotFoundError(f"state file not found: {ctx.state_path}")
        doc = json.loads(ctx.state_path.read_text(encoding="utf-8"))
        ctx.state_doc = doc
        ctx.previous_state_parsed = True
        # Optionally compute comparisons here or in dedicated actions
        prev_in = doc.get("initial_signature_hash") or doc.get("init_sig_hex")
        prev_out = doc.get("output_signature_hash")  or doc.get("output_sig_hex")
        # supply current values via your own functions
        # set to None if unavailable yet
        ctx.input_sig_matches = (prev_in == get_current_input_sig())
        if _exists(ctx.manifest_path):
            ctx.output_hash_matches = (prev_out == get_current_manifest_hash())
        else:
            ctx.output_hash_matches = False
    except Exception as e:
        ctx.error_msg = f"state parse failed: {e}"
        raise

@action(DecisionSignal.GENERATE_NEW_OUTPUT)
def _generate(ctx: Context) -> None:
    # call your existing write_manifest_and_state(...)
    #write_manifest_and_state_for_ctx(ctx)   # you implement
    # after successful rebuild, you can mark matches True
    ctx.input_sig_matches = True
    ctx.output_hash_matches = True

@action(DecisionSignal.SKIP_COMPUTATION)
def _skip(ctx: Context) -> None:
    # no-op here; your outer orchestrator can record success/finish_run
    pass