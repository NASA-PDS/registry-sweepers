#! /usr/bin/env python3
from pds.registrysweepers.driver import run as run_sweepers

# To reduce potential for error when deployed, this file has been left after extraction of contents to
# registrysweepers.driver.run()
# TODO: remove this dependency and have docker run driver.py directly
if __name__ == '__main__':
    run_sweepers()
