import os
import argparse
from user_simulator.transactions_workload_simulator import TransactionsWorkloadSimulator
from romv_scheduler import ROMVScheduler
from serial_scheduler import SerialScheduler
from scheduler_interface import SchedulerInterface
from logger import Logger
from utils import add_feature_to_parser


# This is the main driver to run and test workloads.
# It has a few options for testing and logging:
#     Using certain test files.  [--tests test_file1 [test_file2 [...]]]
#     Forcing using a certain scheduler:
#        [--sched=by-test] / [--sched=romv-rr] / [--sched=romv-serial] / [--sched=simple-serial] / [--sched=compare-all]
#     Run the chosen tests using all 3 schedulers and compare their intermediate and final results [--sched=compare-all]
#     Options for logging:
#        --log-variables            Print the variables values after each change.
#        --log-locks                Print the locks table whenever a lock is acquired or released.
#        --log-wait-for             Print enumeration of the the transactions that an operation waits for.
#        --log-deadlock-cycle       Print a found deadlock cycle caused resetting the transaction.
#        --log-gc                   Print when the GC marks a version to be evicted and when actual eviction happens.
#        --log-transaction-state    Print the transaction state before each attempt to perform an operation.
#        --log-oded-style           Use Oded's style for printing the log lines.
#        --log-sched-prefix         Print the scheduling type in the right side of each printed run-log line.
#        --log-serialization-point  Print the serialization point of each transaction.
#        Some of these are set by default. Use the `--help` to see which.
#        Each such option --log-* has also a --no-log-* version to explicitly turn off this kind of logging.
# Please run `main.py --help` to see a full description of all of the available options.


TESTS_DIR = 'tests'
DEFAULT_TEST_FILENAME = 'transactions.dat'
FORCED_SCHEDULING_TYPES = ['by-test', 'romv-rr', 'romv-serial', 'simple-serial', 'compare-all']


# Helper function to parse the (optional) arguments for this script.
def args_parser():
    parser = argparse.ArgumentParser(
        description='DB implementation [236510] / Simulator for ROMV transactions scheduler.')
    parser.add_argument('--sched', '-s',
                        type=str.lower,
                        choices=FORCED_SCHEDULING_TYPES,
                        default='by-test',
                        help="""
Force using a certain scheduler, regardless of the chosen scheduling scheme mentioned in the test file.
For the ROMV scheduler use either `romv-rr` or `romv-serial` according to the wanted scheduling scheme (serial/RR). 
For the simple serial scheduler use `simple-serial`.
For both `romv-serial` and `simple-serial` you can use the `serial-order` argument to specify the wanted order of
transactions.
Use `compare-all` option to run each test firstly using the ROMV-scheduler with RR scheduling-scheme, and than compare
its run results with both the ROMV-scheduler with serial scheduling-scheme and with the simple serial scheduler, both
using the same transactions order as determined the the first run (using the ROMV-scheduler with RR scheduling-scheme).
If not specified, the ROMV scheduler is used with the scheduling scheme (RR/serial) mentioned in the test file.
""")
    parser.add_argument('--tests', '--test', '-t',
                        type=str, nargs='+', required=False,
                        help='Test file-names to use. If not given, the driver would use the file `{filename}` in the working directory if such a file exists. Otherwise, it will run all the test in the folder `tests/`.'.format(
                            filename=DEFAULT_TEST_FILENAME, tests_dir=TESTS_DIR))

    # Add logging options.
    add_feature_to_parser(parser, ['--log-variables', '-lv'], default=False,
                          help='Verbose mode. Use in order to print the variables values after each change.')
    add_feature_to_parser(parser, ['--log-locks', '-ll'], default=True,
                          help='Verbose mode. Use in order to print the locks table whenever a lock is acquired or released.')
    add_feature_to_parser(parser, ['--log-wait-for', '-lw'], default=True,
                          help='Verbose mode. Use in order to print enumeration of the the transactions that an operation waits for.')
    add_feature_to_parser(parser, ['--log-deadlock-cycle', '-ldlc'], default=True,
                          help='Verbose mode. Use in order to print a found deadlock cycle caused resetting the transaction.')
    add_feature_to_parser(parser, ['--log-gc', '-lgc'], default=True,
                          help='Verbose mode. Use in order to print when the GC marks a version to be evicted and when actual eviction happens.')
    add_feature_to_parser(parser, ['--log-transaction-state', '-lts'], default=True,
                          help='Verbose mode. Use in order to print the transaction state before each attempt to perform an operation.')
    add_feature_to_parser(parser, ['--log-oded-style', '-los'], default=False,
                          help='Use in order to use Oded\'s style for printing the log lines.')
    add_feature_to_parser(parser, ['--log-sched-prefix'], default=True,
                          help='Use in order to print the scheduling type in the right side of each printed run-log line.')
    add_feature_to_parser(parser, ['--log-serialization-point'], default=True,
                          help='Use in order to print the serialization point of each transaction.')
    add_feature_to_parser(parser, ['--log-active-trans'], default=False,
                          help='Use in order to print the currently active transactions in the system, after each operation attempt.')
    add_feature_to_parser(parser, ['--log-serialized-trans'], default=False,
                          help='Use in order to print the currently active transactions that assigned with timestamp, after each operation attempt.')
    add_feature_to_parser(parser, ['--log-locks-table'], default=False,
                          help='Use in order to print the locks table, after each operation attempt.')
    add_feature_to_parser(parser, ['--log-wait-for-graph'], default=False,
                          help='Use in order to print the wait-for graph of the locks manager, after each operation attempt.')

    log_all_or_none = parser.add_mutually_exclusive_group(required=False)
    log_all_or_none.add_argument('--log-all', '-la', action='store_true', default=False,
                                 help='Turn on all logs.')
    log_all_or_none.add_argument('--log-none', '-ln', action='store_true', default=False,
                                 help='Turn off all logs.')

    return parser.parse_args()


def run_workload_simulator_on_scheduler(simulator: TransactionsWorkloadSimulator, scheduler: SchedulerInterface,
                                        test_str_len: int, forced_run_order=None):
    scheduler_type_str = 'ROMV ' + scheduler.scheduling_scheme if isinstance(scheduler,
                                                                             ROMVScheduler) else 'simple-serial'

    # Tell the logger to print the type of the current scheduler type as a prefix of each line of the run-log.
    indent_size = 6
    prefix = ''
    if Logger().is_log_type_set_on('sched_prefix'):
        prefix = scheduler_type_str
        prefix = prefix + ' ' * (16 - len(prefix)) + '|  '
        indent_size = len(prefix)

    # Header for the scheduler type.
    nr_dashes = int((test_str_len - 4 - len(scheduler_type_str) - indent_size * (1 if Logger().is_log_type_set_on('sched_prefix') else 2)) / 2)
    Logger().log((' ' * indent_size) + ('-' * nr_dashes) + '  ' + scheduler_type_str + '  ' + ('-' * nr_dashes))
    Logger().log()

    # Tell the logger to print the type of the current scheduler type as a prefix of each line of the run-log.
    Logger().prefix = prefix

    # Firstly, completely run the first transaction (T0), to fill the variables with some initial value.
    simulator.add_initialization_transaction_to_scheduler(scheduler)
    if forced_run_order is None:
        scheduler.run()

    # Print a blank line after the initialization.
    if not Logger().is_log_type_set_on('oded_style'):
        Logger().log()

    # Run the transactions T1-Tn using the chosen scheduling scheme.
    # After an operation is completed for a certain transaction, the simulator would immediately spawn
    # the next operation (if there is one) to the transaction. The next time the scheduler would encounter
    # this transaction, it will contain this next operation (that has been added previously by the simulator).
    # The only exception is after a commit operation. In that case, after successfully executing this operation,
    # the scheduler would remove the completed transaction from its transactions list, so it would not encounter
    # it anymore. The scheduler runs until all transactions are completed.
    simulator.add_workload_to_scheduler(scheduler)
    scheduler.run(forced_run_order=forced_run_order)

    # Print a blank line in the end of the run.
    Logger().log()

    # Print the data:
    Logger().log("Data in the end of the run:")
    # variables = sorted(list(scheduler.get_variables()), key=lambda x: x[0])
    Logger().log(str(dict(scheduler.get_variables())))

    if scheduler.scheduling_scheme == 'RR':
        Logger().log("Serialization order:")
        Logger().log(str(list(scheduler.get_serialization_order())))

    # Turn off the logger prefix.
    Logger().prefix = ''


def run_scheduling_test(forced_scheduling_type, test_file_path):
    assert forced_scheduling_type in FORCED_SCHEDULING_TYPES

    # Print indication for the begin of the current test.
    test_str = '/'*22 + ' BEGIN TEST: `{}` '.format(test_file_path) + '\\'*22
    test_str_len = len(test_str)
    Logger().prefix = ''
    Logger().log('*' * test_str_len)
    Logger().log(test_str)
    Logger().log()

    # The simulator is responsible for reading the workload test file and injecting the
    # transactions into the scheduler. For each transaction, the simulator simulates an
    # execution of the user program that manages this transaction. It means that values
    # that are retrieved using read-operations might be stored temporarily in local
    # variables of the program, and might be used later for as a value to write in a
    # write-operation. The simulator is also responsible for restarting a transaction
    # that ahs been aborted by the scheduler (due to a deadlock).
    simulator = TransactionsWorkloadSimulator()

    # Parse the workload test file and add its contents to the simulator.
    simulator.load_test_data(test_file_path)

    if forced_scheduling_type != 'compare-all':
        # Initialize the relevant scheduler.
        # By default use the scheduling scheme mentioned in the test file.
        # If a certain scheduling scheme mentioned explicitly in the arguments, use it.
        romv_schedule_scheme = simulator.schedule
        if forced_scheduling_type == 'romv-rr':
            romv_schedule_scheme = 'RR'
        elif forced_scheduling_type == 'romv-serial':
            romv_schedule_scheme = 'serial'
        scheduler = ROMVScheduler(romv_schedule_scheme) if forced_scheduling_type != 'simple-serial' else SerialScheduler()

        # Run the workload using the chosen scheduler.
        run_workload_simulator_on_scheduler(simulator, scheduler, test_str_len)

    elif forced_scheduling_type == 'compare-all':

        # Here we run the workload on all of our 3 schedulers and compare the results.
        # We expect for identical results, as will be explained below.

        # Run the ROMV-scheduler using round-robin scheduling scheme.
        romv_rr_simulator = simulator.clone()
        romv_rr_scheduler = ROMVScheduler('RR')
        run_workload_simulator_on_scheduler(romv_rr_simulator, romv_rr_scheduler, test_str_len)
        romv_rr_serialization_order = list(romv_rr_scheduler.get_serialization_order())

        Logger().log()

        # Run the ROMV-scheduler using serial scheduling scheme.
        romv_serial_simulator = simulator.clone()
        romv_serial_scheduler = ROMVScheduler('serial')
        run_workload_simulator_on_scheduler(romv_serial_simulator, romv_serial_scheduler, test_str_len,
                                            forced_run_order=romv_rr_serialization_order)

        Logger().log()

        # Run the simple serial scheduler.
        simple_serial_simulator = simulator.clone()
        simple_serial_scheduler = SerialScheduler()
        run_workload_simulator_on_scheduler(simple_serial_simulator, simple_serial_scheduler, test_str_len,
                                            forced_run_order=romv_rr_serialization_order)

        # Compare the local variables of all of the operations of all of the transaction of the 3 operation simulators.
        TransactionsWorkloadSimulator.compare_runs(romv_rr_simulator, romv_serial_simulator, simple_serial_simulator)

        # Compare results (variables) of `romv_rr_scheduler`, `romv_serial_scheduler` and `simple_serial_scheduler`.
        SchedulerInterface.compare_results(romv_rr_scheduler, romv_serial_scheduler, simple_serial_scheduler)

    # Print two blank lines to indicate the end of each test.
    Logger().prefix = ''
    Logger().log()
    test_str = '\\' * 22 + ' END TEST: `{}` '.format(test_file_path) + '/' * 22
    Logger().log(test_str)
    Logger().log('*' * len(test_str))
    Logger().log()
    Logger().log()


if __name__ == '__main__':
    # Parse all input (optional) arguments for the scripts.
    args = args_parser()

    # Tell the logger about all of the arguments related to the logging styling.
    for arg_name, arg_value in vars(args).items():
        if not arg_name.startswith('log_'):
            continue
        if arg_name != 'log_all' and arg_name != 'log_none' and arg_name != 'log_oded_style':
            if args.log_all:
                arg_value = True
            elif args.log_none:
                arg_value = False
        Logger().toggle_log_type(arg_name[4:], arg_value)

    # If the tests to run had been explicitly mentioned as an argument, use it.
    # Otherwise, run all of the test files under the `test/` directory.
    test_files = args.tests
    if not test_files and os.path.isfile(DEFAULT_TEST_FILENAME):
        test_files = [DEFAULT_TEST_FILENAME]
    if not test_files:
        test_files = [os.path.join(TESTS_DIR, filename)
                      for filename in os.listdir(TESTS_DIR)
                      if filename and filename[0] != '.'
                      and os.path.isfile(os.path.join(TESTS_DIR, filename))]
    assert isinstance(test_files, list)

    # Run the tests one-by-one.
    for test_file_path in test_files:
        run_scheduling_test(args.sched, test_file_path)
