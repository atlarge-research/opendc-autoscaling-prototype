from StringIO import StringIO

import pandas as pd

from core.Task import Task
from core.Workflow import Workflow
from tests.TestBase import BaseTest
from utils import SimUtils


class TestWorkflowProerties(BaseTest):
    def test_simple_cp(self):
        """
        Two tasks at ts_submit 0. Task 1 is dependent on 1, both have a runtime of 1, so CP = 2.
        """
        task1 = Task(0, 0, 0, 1, 1, set(), 0)
        task2 = Task(1, 0, 0, 1, 1, {0}, 0)

        workflow = Workflow(0, 1, [task1, task2])

        self.assertEquals(SimUtils.calculate_critical_path_length(workflow), 2)
        self.assertEquals(SimUtils.calculate_critical_path_length2(workflow), (2,2))

    def test_simple_cp_ts_1(self):
        """
        Two tasks at ts_submit 1. Task 1 is dependent on 1, both have a runtime of 1, so CP = 2.
        """
        task1 = Task(0, 1, 0, 1, 1, set(), 0)
        task2 = Task(1, 1, 0, 1, 1, {0}, 0)

        workflow = Workflow(0, 1, [task1, task2])

        self.assertEquals(SimUtils.calculate_critical_path_length(workflow), 2)
        self.assertEquals(SimUtils.calculate_critical_path_length2(workflow), (2,2))

    def test_different_ts_submit_cp(self):
        """
        Two tasks at ts_submit 0 and 2. Task 1 is dependent on 1, both have a runtime of 1, so CP = 3.
        """
        task1 = Task(0, 0, 0, 1, 1, set(), 0)
        task2 = Task(1, 2, 0, 1, 1, {0}, 0)

        workflow = Workflow(0, 1, [task1, task2])

        self.assertEquals(SimUtils.calculate_critical_path_length(workflow), 3)
        self.assertEquals(SimUtils.calculate_critical_path_length2(workflow), (3,2))

    def test_complicated_workflow_cp(self):
        """
        Testing workflow 1885 from the Askalon EE trace.
        """

        snippet =  '''
1885      , 41281     , 59        , 515       , 1         , 1         ,           
1885      , 41282     , 59        , 554       , 1         , 1         ,           
1885      , 41283     , 59        , 1714      , 1         , 1         ,           
1885      , 41284     , 2010      , 1714      , 1         , 1         ,           
1885      , 41285     , 3727      , 0         , 1         , 1         ,           
1885      , 41286     , 60        , 587       , 1         , 1         ,           
1885      , 41287     , 59        , 567       , 1         , 1         ,           
1885      , 41288     , 59        , 669       , 1         , 1         ,           
1885      , 41289     , 59        , 0         , 1         , 1         ,           
1885      , 41290     , 59        , 1749      , 1         , 1         ,           
1885      , 41291     , 2075      , 1739      , 1         , 1         ,           
1885      , 41292     , 3817      , 0         , 1         , 1         ,           
1885      , 41293     , 59        , 566       , 1         , 1         ,           
1885      , 41294     , 787       , 566       , 1         , 1         ,           
1885      , 41295     , 59        , 1560      , 1         , 1         ,           
1885      , 41296     , 1701      , 1660      , 1         , 1         ,           
1885      , 41297     , 3374      , 0         , 1         , 1         ,           
1885      , 41298     , 59        , 567       , 1         , 1         ,           
1885      , 41299     , 59        , 1000      , 1         , 1         ,           
1885      , 41300     , 1170      , 920       , 1         , 1         ,           
1885      , 41301     , 2093      , 920       , 1         , 1         ,           
1885      , 41302     , 3016      , 0         , 1         , 1         ,           
1885      , 41303     , 59        , 1983      , 1         , 1         ,           
1885      , 41304     , 59        , 840       , 1         , 1         ,           
1885      , 41305     , 1010      , 836       , 1         , 1         ,           
1885      , 41306     , 1849      , 836       , 1         , 1         ,           
1885      , 41307     , 2730      , 21        , 1         , 1         ,           
1885      , 41308     , 60        , 1641      , 1         , 1         ,           
1885      , 41309     , 1908      , 1672      , 1         , 1         ,           
1885      , 41310     , 3592      , 0         , 1         , 1         ,           
1885      , 41311     , 2761      , 21        , 1         , 1         , 41299 41293 41282 41281 41308 41304 41286 41283 41289 41288 41298 41287 41290 41295 41303'''
        df = pd.read_csv(StringIO(snippet), delimiter="\s*,\s*",
                         skipinitialspace=True, keep_default_na=False, header=None)

        tasks = []
        for item in df.itertuples():
            task = Task(item[2], item[3], 0, item[4], item[5], set(int(a) for a in item[7].split()), 0)
            tasks.append(task)

        workflow = Workflow(0, 1885, tasks)
        self.assertEquals(SimUtils.calculate_critical_path_length(workflow), 3759)
        self.assertEquals(SimUtils.calculate_critical_path_length2(workflow), (3759, 1))
