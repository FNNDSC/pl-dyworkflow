#!/usr/bin/env python

from pathlib import Path
from argparse import ArgumentParser, Namespace, ArgumentDefaultsHelpFormatter

from chris_plugin import chris_plugin, PathMapper
import  os, sys
os.environ['XDG_CONFIG_HOME'] = '/tmp'

import  pudb
from    pudb.remote             import set_trace
from    loguru                  import logger
from    concurrent.futures      import ThreadPoolExecutor, ProcessPoolExecutor
from    threading               import current_thread, get_native_id

from    typing                  import Callable, Any, Iterable, Iterator
from    io                      import TextIOWrapper

from    datetime                import datetime, timezone
import  json
from    state                   import data
from    logic                   import behavior
from    control                 import action
from    control.filter          import PathFilter
from    pftag                   import pftag
from    pflog                   import pflog

LOG             = logger.debug

logger_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> │ "
    "<level>{level: <5}</level> │ "
    "<yellow>{name: >28}</yellow>::"
    "<cyan>{function: <30}</cyan> @"
    "<cyan>{line: <4}</cyan> ║ "
    "<level>{message}</level>"
)
logger.remove()
logger.add(sys.stderr, format=logger_format)

timenow:Callable[[], str]       = lambda:\
        datetime.now(timezone.utc).astimezone().isoformat()

ld_forestResult:list            = []

__version__ = '1.1.1'

DISPLAY_TITLE = r"""
       _           _                          _     __ _
      | |         | |                        | |   / _| |
 _ __ | |______ __| |_   ___      _____  _ __| | _| |_| | _____      __
| '_ \| |______/ _` | | | \ \ /\ / / _ \| '__| |/ /  _| |/ _ \ \ /\ / /
| |_) | |     | (_| | |_| |\ V  V / (_) | |  |   <| | | | (_) \ V  V /
| .__/|_|      \__,_|\__, | \_/\_/ \___/|_|  |_|\_\_| |_|\___/ \_/\_/
| |                   __/ |
|_|                  |___/
"""

parser: ArgumentParser      = ArgumentParser(
    description = '''
A ChRIS plugin that splits its parent node into multiple children
and then applies a workflow to each child.
''',
    formatter_class=ArgumentDefaultsHelpFormatter)

parser.add_argument('-V', '--version', action='version',
                    version=f'%(prog)s {__version__}')

parser.add_argument(
            '--pattern',
            default = '**/*dcm',
            help    = '''
            pattern for file names to include (you should quote this!)
            (this flag triggers the PathMapper on the inputdir).'''
)
parser.add_argument(
            '--pluginInstanceID',
            default = '',
            help    = 'plugin instance ID from which to start analysis'
)
parser.add_argument(
            '--CUBEurl',
            default = 'http://localhost:8000/api/v1/',
            help    = 'CUBE URL'
)
parser.add_argument(
            '--CUBEuser',
            default = 'chris',
            help    = 'CUBE/ChRIS username'
)
parser.add_argument(
            '--CUBEpassword',
            default = 'chris1234',
            help    = 'CUBE/ChRIS password'
)
parser.add_argument(
            '--orthancURL',
            default = 'https://orthanc-chris-public.apps.ocp-prod.massopen.cloud/',
            help    = 'IP of the orthanc to receive analysis results'
)
parser.add_argument(
            '--orthancuser',
            default = 'fnndsc',
            help    = 'Orthanc username'
)
parser.add_argument(
            '--orthancpassword',
            default = 'Lerkyacyids5',
            help    = 'Orthanc password'
)
parser.add_argument(
            '--orthancremote',
            default = '',
            help    = 'remote orthanc modality'
)
parser.add_argument(
            '--pipeline',
            default = '',
            help    = 'pipeline to attach to each child node'
)
parser.add_argument(
            '--pipelineParamFile',
            default = '',
            help    = 'yaml formatted file of pipeline parameters'
)
parser.add_argument(
            '--blockOnNode',
            default = '',
            help    = 'optional node on which the controller will wait/block. Useful to measure execution time'
)
parser.add_argument(
            '--verbosity',
            default = '0',
            help    = 'verbosity level of app'
)
parser.add_argument(
            "--thread",
            help    = "use threading to branch in parallel",
            dest    = 'thread',
            action  = 'store_true',
            default = False
)
parser.add_argument(
            "--pftelDB",
            help    = "an optional pftel telemetry logger, of form '<pftelURL>/api/v1/<object>/<collection>/<event>'",
            default = ''
)
parser.add_argument(
            "--inNode",
            help    = "perform in-node implicit parallelization in conjunction with --thread",
            dest    = 'inNode',
            action  = 'store_true',
            default = False
)
parser.add_argument(
            "--notimeout",
            help    = "if specified, then controller never timesout while waiting on nodes to complete",
            dest    = 'notimeout',
            action  = 'store_true',
            default = False
)
parser.add_argument(
            "--debug",
            help    = "if true, toggle telnet pudb debugging",
            dest    = 'debug',
            action  = 'store_true',
            default = False
)
parser.add_argument(
            "--debugTermSize",
            help    = "the terminal 'cols,rows' size for debugging",
            default = '253,62'
)
parser.add_argument(
            "--debugPort",
            help    = "the debugging telnet port",
            default = '7900'
)
parser.add_argument(
            "--debugHost",
            help    = "the debugging telnet host",
            default = '0.0.0.0'
)

def Env_setup(  options         : Namespace,
                inputdir        : Path,
                outputdir       : Path,
                debugPortOffset : int = 0) -> data.env:
    """
   Setup the environment

    Args:
        options (Namespace):    options passed from the CLI caller
        inputdir (Path):        plugin global input directory
        outputdir (Path):       plugin global output directory
        debugPortOffset (int, optional): offset added to debug port -- useful for multithreading. Defaults to 0.

    Returns:
        data.env: an instantiated environment object. Note in multithreaded
                  runs, each thread gets its own object.
    """
    Env: data.env               = data.env()
    Env.CUBE.set(inputdir       = str(inputdir))
    Env.CUBE.set(outputdir      = str(outputdir))
    Env.CUBE.set(url            = str(options.CUBEurl))
    Env.CUBE.set(username       = str(options.CUBEuser))
    Env.CUBE.set(password       = str(options.CUBEpassword))
    Env.orthanc.set(url         = str(options.orthancURL))
    Env.orthanc.set(username    = str(options.orthancuser))
    Env.orthanc.set(password    = str(options.orthancpassword))
    Env.set(inputdir            = inputdir)
    Env.set(outputdir           = outputdir)
    Env.debug_setup(    debug       = options.debug,
                        termsize    = options.debugTermSize,
                        port        = int(options.debugPort) + debugPortOffset,
                        host        = options.debugHost
    )
    return Env

def preamble(options: Namespace) -> str:
    """
    Just show some preamble "noise" in the output terminal and also process
    the --pftelDB if provided.

    Args:
        options (Namespace): CLI options namespace

    Returns:
        str: the parsed <pftelDB> string
    """

    print(DISPLAY_TITLE)
    pftelDB:str     = ""

    if options.pftelDB:
        tagger:pftag.Pftag  = pftag.Pftag({})
        pftelDB             = tagger(options.pftelDB)['result']

    LOG("plugin arguments...")
    for k,v in options.__dict__.items():
         LOG("%25s:  [%s]" % (k, v))
    LOG("")

    LOG("base environment...")
    for k,v in os.environ.items():
         LOG("%25s:  [%s]" % (k, v))
    LOG("")

    LOG("Starting growth cycle...")
    return pftelDB

def childFilter_build(options: Namespace, Env : data.env) -> action.PluginRun:
    """
    Return a filter object that will be used to filter one child from the
    parent.

    Args:
        options (Namespace): options namespace
        Env (data.env): the environment for this tree

    Returns:
        action.PluginRun: A filter specific to this tree that will
                          filter a study of interest in the parent
                          space -- analogously akin to choosing a
                          seed.
    """

    LOG("Building filter in thread %s..." % get_native_id())
    LOG("Constructing object to filter parent field")
    PLinputFilter: action.PluginRun = action.PluginRun(
                                        env     = Env,
                                        options = options
                                    )

    if len(options.pluginInstanceID):
        Env.CUBE.parentPluginInstanceID  = options.pluginInstanceID
    else:
        Env.CUBE.parentPluginInstanceID  = \
            Env.CUBE.parentPluginInstanceID_discover()['parentPluginInstanceID']
    return PLinputFilter

def respawnChild_catchError(PLseed:action.PluginRun, input: Path) -> dict:
    """
    Re-run a failed filter (pl-shexec) with explicit error catching

    Args:
        PLseed (action.Pluginrun): the plugin run object to re-execute
        input (Path): the input on which the seed failed

    Returns:
        dict: the detailed error log from the failed run
    """
    global  LOG
    LOG("Some error was returned when planting the seed!")
    LOG('Replanting seed with error catching on...')
    d_seedreplant:dict  = PLseed(str(input), append = "--jsonReturn")
    return d_seedreplant

def childNode_create(options:Namespace, env:data.env, input:Path, d_ret:dict[Any, Any]) -> bool:

    global LOG

    def initLogging_do() -> None:
        nonlocal str_heartbeat
        Path('%s/start-%s.touch' % (env.outputdir.touch(), str_threadName))
        LOG("Processing parent in thread %s..." % str_threadName)
        fl:TextIOWrapper                = open(str_heartbeat, 'w')
        fl.write('Start time: {}\n'.format(timenow()))
        fl.close()
        LOG("Filtering parent->child in %s" % str(input))

    str_threadName:str              = current_thread().getName()
    str_heartbeat:str               = str(env.outputdir.joinpath('heartbeat-%s.log' % \
                                                str_threadName))
    d_ret['heartbeat']              = str_heartbeat
    conditional:behavior.Filter     = behavior.Filter()
    conditional.obj_pass            = behavior.unconditionalPass
    if not conditional.obj_pass(str(input)):
        d_ret['status']             = False
        d_ret['message']            = 'No data in parent was filtered'
        return False

    initLogging_do()

    PLinputFilter:action.PluginRun  = childFilter_build(options, env)
    d_ret['childFilter']            = PLinputFilter(str(input))
    if not d_ret['childFilter']['status']:
        d_ret['childFilter']['debug'] = respawnChild_catchError(PLinputFilter, input)
        return False

    return True

def workflow_attachToChild(options:Namespace, env:data.env, d_ret:dict[Any, Any]) -> bool:

    def endLogging_do() -> None:
        fl:TextIOWrapper                = open(d_ret['heartbeat'], 'w')
        fl.write('End   time: {}\n'.format(timenow()))
        fl.close()

    workflow:action.Workflow        = action.Workflow(env = env, options = options)
    d_ret["workflowRun"]            = workflow(d_ret['childFilter']['branchInstanceID'])
    endLogging_do()
    return True

def parentNode_process(options: Namespace, env:data.env, input: Path, output: Path) -> dict:
    """
    The "main" function of this plugin.

    Based on some conditional applied to the <input> file space, direct the
    dynamic "growth" of this feed tree from the parent node of *this* plugin.

    Args:
        options (Namespace): CLI options
        input (Path): input path returned by mapper
        output (Path, optional): output path returned by mapper. Defaults to None.

    Returns:
        dict: resultant object dictionary of this (threaded) growth
    """

    global LOG, ld_forestResult

    def init_vars() ->  dict[Any, Any]:
        d_childFilter:dict[Any, Any]    = {
            "status"        : False,
            "message"       : "",
            "error"         : "unable to filter child",
            "debug"         : {}
        }
        d_worflowRun:dict[Any, Any]     = {
            "status"        : False,
            "message"       : "",
            "error"         : "unable to attach workflow to child"
        }
        d_ret:dict                      = {
            "status"        : False,
            "message"       : "",
            "heartbeat"     : "",
            "childFilter"   : d_childFilter,
            "workflowRun"   : d_worflowRun
        }
        return d_ret

    # set_trace(term_size=(253, 62), host = '0.0.0.0', port = 7900)
    env.set_telnet_trace_if_specified()
    d_ret:dict[Any, Any]            = init_vars()

    if not childNode_create(options, env, input, d_ret):
        return d_ret

    if workflow_attachToChild(options, env, d_ret):
        ld_forestResult.append(d_ret)

    # This global variable is accessed/used to record the results
    # of multijob runs
    return d_ret

def mapper_resolve(options:Namespace, inputdir:Path, outputdir:Path) -> PathMapper:
    """
    Simply creates and returns a mapper -- either a dir_mapper_deep or a
    file_mapper depending on options settings.

    Args:
        options (Namespace): CLI namespace
        inputdir (Path): inputdir of plugin
        outputdir (Path): outputdir of plugin

    Returns:
        PathMapper: a mapper as parameterized by options
    """
    mapper:PathMapper   = PathMapper.dir_mapper_deep(inputdir, outputdir)
    if not options.inNode:
        mapper          = PathMapper.file_mapper(
                            inputdir,
                            outputdir,
                            glob        = options.pattern
                        )
    return mapper

def multijob_handle(options:Namespace, env:data.env, mapper:PathMapper) -> bool:
    if int(options.thread):
        with ThreadPoolExecutor(max_workers=len(os.sched_getaffinity(0))) as pool:
            presults:Iterator[dict[Any, Any]] = pool.map(lambda t:
                                        parentNode_process(options, env, *t),
                                                            mapper)

        # raise any Exceptions which happened in threads
        for _ in presults:
            pass
        return True
    return False


# The main function of this *ChRIS* plugin is denoted by this ``@chris_plugin`` "decorator."
# Some metadata about the plugin is specified here. There is more metadata specified in setup.py.
#
# documentation: https://fnndsc.github.io/chris_plugin/chris_plugin.html#chris_plugin
@chris_plugin(
    parser              = parser,
    title               = 'Dynamic Workflow Controller',
    category            = '',         # ref. https://chrisstore.co/plugins
    min_memory_limit    = '100Mi',    # supported units: Mi, Gi
    min_cpu_limit       = '1000m',    # millicores, e.g. "1000m" = 1 CPU core
    min_gpu_limit       = 0           # set min_gpu_limit=1 to enable GPU
)
@pflog.tel_logTime(
    event               = 'dyworkflow',
    log                 = 'Leg Length Discepency Dynamic Workflow controller'
)
def main(options: Namespace, inputdir: Path, outputdir: Path):
    """
    *ChRIS* plugins usually have two positional arguments: an **input directory** containing
    input files and an **output directory** where to write output files. Command-line arguments
    are passed to this main method implicitly when ``main()`` is called below without parameters.

    :param options: non-positional arguments parsed by the parser given to @chris_plugin
    :param inputdir: directory containing (read-only) input files
    :param outputdir: directory where to write output files
    """

    options.pftelDB             = preamble(options)
    d_results:dict[Any, Any]    = {}
    env:data.env                = Env_setup(options,
                                            inputdir,
                                            outputdir,
                                            get_native_id())


    mapper:PathMapper = mapper_resolve(options, inputdir, outputdir)
    if not multijob_handle(options, env, mapper):
        for input, output in mapper:
            d_results =   parentNode_process(options, env, input, output)
        print(d_results)

if __name__ == '__main__':
    main()
