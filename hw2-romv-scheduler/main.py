
import argparse
from user_simulator import TransactionsWorkloadSimulator
from scheduler_modules import ROMVScheduler, SerialScheduler


def args_parser():
    parser = argparse.ArgumentParser(
        description='DB implementation [236510] / Simulator for ROMV transactions scheduler.')
    parser.add_argument('--sched', '-s',
                        type=str, nargs='?', choices=['RR', 'serial'], default='RR',
                        help='Scheduler to use. `RR` for round-robin, or `serial` for serial scheduler.')
    return parser.parse_args()


if __name__ == '__main__':
    args = args_parser()
    assert args.sched in {'RR', 'serial'}
    scheduler_type = ROMVScheduler if args.sched == 'RR' else SerialScheduler

    scheduler = scheduler_type()
    simulator = TransactionsWorkloadSimulator()
    simulator.load_test_data(workload_data_filename='transactions.dat')
    simulator.add_initialization_transaction_to_scheduler(scheduler)
    scheduler.run()
    simulator.add_workload_to_scheduler(scheduler)
    scheduler.run()
