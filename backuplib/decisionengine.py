from typing import Callable, Iterable, Tuple, TypeVar, Generic, List, FunctionType, NamedTuple, Dict
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from zmq.decorators import context
from logging import Logger


class DecisionMaker:
    Outcomes: Enum
    rules: List[Tuple[str, FunctionType, Outcomes]]
    
    
# ---- side-effect actions (dispatcher will call these) ----
Action = Callable["Context", None]
    

# Generic decision engine ----------------------------------
TOutcome = TypeVar("TOutcome")


class DecisionSignal(Enum):
    CONTINUE = auto()
    REPEAT_LOOP = auto()
    SKIP_JOB_RUN = auto()
    LOAD_PREVIOUS_OUTPUT = auto()
    GENERATE_NEW_OUTPUT = auto()
    LOAD_PREVIOUS_RUN_STATE = auto()
    PARSE_PREVIOUS_STATE = auto()
    COMPUTE_CURRENT_SIGNATURE = auto()
    ENCOUNTERED_ERR = auto()
    ERROR_HALT = auto()
    GENERATE_NEW_STATE = auto()
    GENERATE_NEW_STATE_HASH = auto()
    GENERATE_ERROR_TRACE = auto()
    END = auto()


Predicate = Callable[["Context"], bool]

class ConditionalDo(NamedTuple):
    name: str
    when: Predicate
    signal: DecisionSignal
    
class DoAction(NamedTuple):
    name: str
    when: Predicate
    signal: DecisionSignal

class ProgramStage(NamedTuple):
    stage_id : str
    stage_name: str
    rules: List[ConditionalDo | DoAction]
    contract: Predicate

ProgramPlan = List[ProgramStage]
ConditionalDo = Tuple[str, Predicate, DecisionSignal]

class ProgramLogicRunner:
    
    def __init__(self, context):
        self.context = context
        self.stage_signal_switch = {}
        self.DISPATCH : Dict[DecisionSignal, Action] = {}
        self.program_plan: ProgramPlan = []
        self.logger = None
        
    def set_logger(self, logger: Logger):
        self.logger = logger
                
    def set_program_plan(self, program_plan : ProgramPlan):
        self.program_plan = program_plan
    
    def set_stage_signal_switch(self, stage_signal_switch):
        self.stage_signal_switch = stage_signal_switch
    
    
    def action(self, signal: DecisionSignal):
        def wrap(fn: Action):
            self.DISPATCH[signal] = fn
            return fn
        return wrap
    
    def _get_stage_by_id(self, stage_id):
        return next(p for p in self.program_plan if p.stage_id == stage_id)
    
    def _check_switch_rules(self, current_stage, signal) -> Tuple[bool, ProgramStage]:
        stage_switch_rules = self.stage_signal_switch.get(current_stage.stage_id, None)
        if stage_switch_rules is not None:
            stage_switch = stage_switch_rules.get(signal, None)
            if stage_switch is not None:
                new_stage = self._get_stage_by_id(stage_switch)
                return True, new_stage
        return False, None
    

    def _get_next_stage(self, current_stage : ProgramStage):
        stages : List[ProgramStage] = self.program_plan
        i = stages.index(current_stage)
        next_stage = stages[i+1] if i+1 < len(stages) else None
        return next_stage


    def _exec_loop(self, current_stage, rule, trace, signal = DecisionSignal.CONTINUE):
        
        stage_id = current_stage.stage_id
        trace.append(f"stage_id: {stage_id}")
        self.logger.info(f"stage {current_stage.stage_id}")
        # add good audit trail logic
        rules = current_stage.rules
        if rule is None:
            r = rules[0]

        trace.append(f"rule: {r.name}")
        if r.when(self.context):
            trace.append(f"rule: {r.name}: true")
            sig = r.signal
                
            is_switch, new_state = self._check_switch_rules(current_stage, sig)
            if is_switch:
                current_stage = new_state
                break
            try:
                self.DISPATCH[sig](self.context)
            except:
                self.logger.exception(f"failed on stage {current_stage.stage_name}")
                
                    
        i = rules.index(r)
        next_rule = rules[i+1] if i+1 < len(rules) else None
        
        if next_rule is not None:
            self._exec_loop(current_stage=current_stage, rule=next_rule, trace=trace, signal=signal)
        
        if not current_stage.contract(self.context):
            self._exec_loop(current_stage=current_stage, rule=None, trace=trace, signal=DecisionSignal.ENCOUNTERED_ERR)
            pass
        
        if next_rule is None:
            self._exec_loop(current_stage=self._get_next_stage(current_stage), rule=None, trace=trace, signal=signal)


            
                
    


    def start_run(
                self,
                ):
        trace = []
        current_stage: ProgramStage = self.program_plan[0]
        

        while True:
            stage_id = current_stage.stage_id
            trace.append(f"stage_id: {stage_id}")
            self.logger.info(f"stage {current_stage.stage_id}")
            # add good audit trail logic
            for r in current_stage.rules:
                trace.append(f"rule: {r.name}")
                if r.when(self.context):
                    trace.append(f"rule: {r.name}: true")
                    sig = r.signal
                        
                    is_switch, new_state = self._check_switch_rules(current_stage, sig)
                    if is_switch:
                        current_stage = new_state
                        break
                    try:
                        self.DISPATCH[sig](self.context)
                    except:
                        self.logger.exception(f"failed on stage {current_stage.stage_name}")
                        
                        # add good error handling
            if not current_stage.contract(self.context):
                pass
                    
class StageName(Enum):
    ALL : auto()
    LOAD : auto()
    COMPSIG : auto()
    CHECK_CUR: auto()
    MAIN : auto()
    NEW_STATE : auto()
    ERR_HANDLING : auto()
    END : auto()
                    

program_stages = [
    ProgramStage(StageName.LOAD, 
                 "checking previous run", [
            ConditionalDo("previous run output not loaded", lambda c: not c.output_loaded, DecisionSignal.LOAD_PREVIOUS_OUTPUT),
            ConditionalDo("no previous run exists", lambda c: not c.is_previous_run, DecisionSignal.GENERATE_NEW_OUTPUT),
            ConditionalDo("full re-run has been executed", lambda c: not c.completed_main_job_run, DecisionSignal.GENERATE_NEW_STATE),
            ConditionalDo("previous run state not loaded", lambda c: not c.state_loaded, DecisionSignal.LOAD_PREVIOUS_RUN_STATE),
            DoAction("parse previous state data", lambda c: c.is_state_parsed, DecisionSignal.PARSE_PREVIOUS_STATE),
        ],
    lambda c : c.output_loaded and c.state_loaded
    ),
    ProgramStage(StageName.COMPSIG, 
                 "computing current signature", [
            DoAction("compute current signature", lambda c: c.current_signature is None,  DecisionSignal.COMPUTE_CURRENT_SIGNATURE),
    ],
    lambda c: c.current_signature is not None 
    ),
    ProgramStage(StageName.CHECK_CUR,
                 "comparing current signature to previous state output signature", [
            ConditionalDo("current signature = previous state out signature", lambda c: c.is_signature_same_as_last_run, DecisionSignal.SKIP_JOB_RUN),
    ],
    lambda c : c.is_signature_same_as_last_run
    ),
    ProgramStage(StageName.MAIN,"perform main job", [
            ConditionalDo("current signature is new", lambda c: c, DecisionSignal.GENERATE_NEW_OUTPUT)
    ],
    lambda c : c.is_new_output_generated
    ),
    ProgramStage(StageName.NEW_STATE,
                 "generate new state file", [
            DoAction("generate state hash", lambda c : c.new_state_hash is not None, DecisionSignal.GENERATE_NEW_STATE_HASH),
            DoAction("generate new state json", lambda c: c.new_state_json is None, DecisionSignal.GENERATE_NEW_STATE_JSON),
        ],
    lambda c : c.new_state_generated
    ),
    ProgramStage(StageName.END,
                 "finished and end", [
            DoAction("end", lambda c : True, DecisionSignal.END),
        ],
    lambda c : True
    ),
    ProgramStage(StageName.ERR_HANDLING,
                 "handle an error", [
            DoAction("generate error logs with trace", lambda c: True is None, DecisionSignal.GENERATE_ERROR_TRACE),
            DoAction("error halt", lambda c : True, DecisionSignal.ERROR_HALT)
        ],
    lambda c : True
    ),
]

SignalSwitch = Dict[StageName, Dict[DecisionSignal, StageName]]

stage_signal_switch: SignalSwitch = {
    StageName.ALL : {
        DecisionSignal.COMPUTE_CURRENT_SIGNATURE: StageName.ERR_HANDLING,
        DecisionSignal.ERROR_HALT: StageName.END
    },
    StageName.LOAD : {
        DecisionSignal.GENERATE_NEW_STATE: StageName.NEW_STATE
    }
    
}


# ---- engine: pure rule loop -> signal -> dispatch; handles errors predictably ----
def run_engine(
                ctx: Context,
                rules: Iterable[ConditionalDo] = decision_rules,
                *,
                max_steps: int = 100
               ) -> DecisionSignal:
    
    for _ in range(max_steps):
        for r in rules:
            if r.when(ctx):
                ctx.trace.append(r.name)
                sig = r.signal
                if sig in (DecisionSignal.SKIP_COMPUTATION, DecisionSignal.ERROR_HALT):
                    return sig
                try:
                    self.DISPATCH[sig](ctx)
                except Exception as e:
                    # normalize any action failure into ERROR_HALT
                    ctx.trace.append(f"ERROR: {sig.name} -> {e}")
                    ctx.error_msg = ctx.error_msg or str(e)
                    return DecisionSignal.ERROR_HALT
                break  # re-scan from top with updated ctx
        else:
            ctx.error_msg = "no rule matched; inconsistent context"
            return DecisionSignal.ERROR_HALT
    ctx.error_msg = "max steps exceeded; possible cycle"
    return DecisionSignal.ERROR_HALT






