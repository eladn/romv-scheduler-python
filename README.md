The code in this repository has been written as part of HW solutions for the course `Database Management Sys. Implementations` [236510] Spring 2018 @ CS faculty Technion. If you use portions of and/or the whole code for academic purposes, please make sure to mention this repository.

# ROMV scheduler
Python implementation for the ROMV scheduler (read-only multi-version) with RR (round robin) scheduling scheme over the given transactions. The implementation follows the definition of `ROMV scheduler` as proposed in the book `Transactional Information Systems: Theory, Algorithms, and the Practice of Concurrency Control and Recovery` in section 5.5.4 page 211 ([link to google book](https://books.google.co.il/books?id=wV5Ran71zNoC&lpg=PP1&pg=PA211#v=onepage&q&f=false)). See full implementation documentation at `hw2-romv-scheduler-documentation-with-chart.pdf`.

# User workload simulator
Transactions workload simulator with UI to demonstrate executions of the scheduler. Run `python3 main.py --help` to see a full description of all available simulating options. 
