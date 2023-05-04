from django.test.runner import DiscoverRunner
import sys


class NoCreateDbTestRunner(DiscoverRunner):
    '''
    This class overrided the default discoverRunner methods to prevent init test creating a new test DB for every unittest.
    A pre-created DB (using "create database _test_sqs_dev template qml_dev owner dev;") is used for the tests.
    (This is because the create db process requires migrations to be run, which fail because of the current sequence for ledger_api_client)

    To run:
        ./manage.py test --testrunner=tests.no_db_test_runner.NoDbTestRunner --settings='sqs.settings_no_db'    

    OR (since testrunner is file declared in sqs/settings_no_db.py)
    NOTE: This will run all unit-tests (filenames begiinning with test_*) in dir tests/ 

        ./manage.py test --settings='sqs.settings_no_db'    
        python -m ipdb -c cont myscript.py

    TEST DB Created/Copied from main DB:
        ./manage.py dbshell
        create database _test_sqs_dev template qml_dev owner dev;
    '''

    def setup_databases(self, **kwargs):
        """ Override the database creation defined in parent class 
            we do not want to create DB, use a pre-existing DB, defined in 'sqs/settings_no_db.py'
        """
        print("Method: setup_databases.")
        pass

    def teardown_databases(self, old_config, **kwargs):
        """ Override the database teardown defined in parent class """
        print("Method: teardown_databases.")
        pass

#    def run_tests(self, test_labels, extra_tests=None, **kwargs):
#        pass
        




def run_tests():
    '''
    An alternative way to run tests, but not used
    From shell_plus:
        from tests.no_db_test_runner import run_tests
        run_tests()

    ---------------------------------------------

    From command-line:
    By default will run 
        ./manage.py test --testrunner=tests.no_db_test_runner.NoDbTestRunner --settings='sqs.settings_no_db'    

    '''
    from django.test.runner import DiscoverRunner
    #runner = DiscoverRunner(failfast=True, verbosity=1)
    #runner = SelectTestClass(verbosity=1)
    runner = NoCreateDbTestRunner(verbosity=1, settings='sqs.settings_no_db')
    failures = runner.run_tests(['tests'], interactive=True)
    sys.exit(failures)

if __name__ == '__main__':
    run_tests()



