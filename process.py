import os
import sqlite3

import yaml


class GitWorker(object):
    
    def __init__(self):
        pass
    
    def get_file(self, repo, file):
        pass
    

class DB(object):
    
    ctable = """
    CREATE TABLE IF NOT EXISTS policies (
    lob TEXT Not Null,
    account TEXT Not Null,
    policyname TEXT Not Null,
    resourcetype TEXT Not Null,
    env TEXT Not Null,
    cloud TEXT Not Null,
    hdw TEXT Not Null,
    filename TEXT Not Null,
    policy BLOB Not Null);
    CREATE INDEX IF NOT EXISTS lidx ON policies(lob);
    CREATE INDEX IF NOT EXISTS aidx ON policies(account);
    CREATE INDEX IF NOT EXISTS pnidx ON policies(policyname);
    CREATE INDEX IF NOT EXISTS ridx ON policies(resourcetype);
    CREATE INDEX IF NOT EXISTS eidx ON policies(env);
    CREATE INDEX IF NOT EXISTS cidx ON policies(cloud);
    CREATE INDEX IF NOT EXISTS hidx ON policies(hdw);
    CREATE INDEX IF NOT EXISTS fidx ON policies(filename);
    """
    
    inselem = """
    insert into policies (lob, account, policyname, resourcetype,
    env, cloud, hdw, filename, policy) values (:lob, :account,
    :policyname, :resourcetype, :env, :cloud, :hdw, :filename, :policy)
    """
    
    def __init__(self, dbname):
        self.db = sqlite3.connect(dbname)
        self.cur = self.db.cursor()
        self.cur.executescript(self.ctable)
        
    def insert(self, **kwargs):
        self.cur.execute(self.inselem, kwargs)
    

class Policies(object):

    def __init__(self, filename):
        self.fd = open(filename)
        self.j = yaml.safe_load(self.fd)
        self.policies = self.j['policies']

    def __iter__(self):
        self.idx = 0
        self.no_policies = len(self.policies)
        return self
    
    def __next__(self):
        if self.idx < self.no_policies:
            self.idx += 1
            return self.policies[self.idx-1]
        else:
            raise StopIteration

    def get_policy(self, policy_name):
        pass

    def list_of_policies(self):
        pass
    
        
class ProcessPolicies(object):
    
    def __init__(self, git, repo, filename, account, division, env, hdw):
        self.fname = fname = git.get_file(github_filename)
        self.p = Policies(fname)
        self.acct = account
        self.div = division
        self.prod_dev_etc = evn
        self.hr_wk_daily = hdw
        
    def process(self, proc):
        i = iter(self.p)
        for pol in i:
            proc(pol, self.acct, self.div, self.prod_dev_etc, self.hr_wk_daily)
        os.remove(self.fname)
            
class Accounts(object):
    
    def __init__(self, stream):
        self.accts = yaml.safe_load(stream)['accounts']
        self.git = GitWorker()
        
    def process_accounts(self, proc):
        for a in self.accts:
            # Process this account
            info = self.accts[a]
            repo = 'config=' + a
            p = ProcessPolicies(self.git, repo, file='hourly.yml', 
                                account=a, division=info['division'], env=info['run_env'], ptype='hourly')
            p.process(proc)
            p = ProcessPolicies(self.git, repo, file='daily.yml',
                                account=a, division=info['division'], env=info['run_env'], ptype='daily')
            p.process(proc)
            p = ProcessPolicies(self.git, repo, file='weekly.yml',
                                account=a, division=info['division'], env=info['run_env'], ptype='weekly')
            p.process(proc)
    

def pname(i, aact, div, env):
    print(i['name'])

db = DB('test3.sql')
db.insert(lob='111', account='222', policyname='333', resourcetype='444',
    env='555', cloud='aws', hdw='hourly', filename='foo.yml', policy="      ")
a = open('a.yml')
accts = Accounts(a)
accts.process_accounts(pname)