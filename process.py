import os
import sqlite3
import io

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
    comment BLOB,
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
    env, cloud, hdw, filename, comment, policy) values (:lob, :account,
    :policyname, :resourcetype, :env, :cloud, :hdw, :filename, 
    :comment, :policy)
    """
    
    def __init__(self, dbname):
        self.db = sqlite3.connect(dbname)
        self.cur = self.db.cursor()
        self.cur.executescript(self.ctable)
        
    def insert(self, **kwargs):
        self.cur.execute(self.inselem, kwargs)
        self.db.commit()
    

class Policies(object):

    def __init__(self, stream):
        self.policies = yaml.safe_load(stream)['policies']

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
    
    def __init__(self, git, repo, account, division, env):
        self.acct = account
        self.div = division
        self.prod_dev_etc = env
        self.git = git
        self.repo = repo
        
        
    def process_policy(self, db, hdw, filename, pol):
        # Simple - process the information. Store it in the database
        name = pol['name']
        rt = pol['resource']
        if 'description' in pol:
            comment = pol['description']
        elif 'comment' in pol:
            comment = pol['comment']
        elif 'comments' in pol:
            comment = pol['comments']
        else:
            comment = ''
        db.insert(lob=self.div, account=self.acct, policyname=name, resourcetype=rt, hdw=hdw,
                  env=self.prod_dev_etc, cloud='aws', filename=filename, comment=comment,
                  policy=yaml.dump(pol) )
                
                
    def process(self, db, file, hdw):
        buf = self.git.get_file(self.repo, file)
        self.p = Policies(buf)        
        i = iter(self.p)
        for pol in i:
            self.process_policy(db, hdw, self.repo + '/' + file, pol)


class Accounts(object):
    
    def __init__(self, repo, file, buf=None):
        # Pull the accounts.yml file down from the repo. And process the 
        # yaml file.
        self.git = GitWorker()
        if not buf:
            buf = self.git.get_file(repo, file)
        self.accts = yaml.safe_load(io.StringIO(buf))['accounts']
        
 
    def process_accounts(self, db):
        # For each account in the accounts.yml file, collect the policies from
        # the three policy files.
        for a in self.accts:
            # Process this account
            info = self.accts[a]
            repo = 'config=' + a
            p = ProcessPolicies(self.git, repo, account=a, division=info['division'], env=info['run_env'])
            p.process(db, 'hourly.yml', 'hourly')
            p.process(db, 'daily.yml', 'daily')
            p.process(db, 'weekly.yml', 'weekly')

    
db = DB('policy.db')
accts = Accounts('janitorial services', 'accounts.yml', buf=open('a.yml').read())
accts.process_accounts(db)