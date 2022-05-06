import gc
import time
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from multiprocessing import Process
from random import choices,choice,random,randrange,randint
from string import ascii_lowercase

from sqlalchemy import (Column, DateTime, Float, Integer, Numeric, String,
                        Text, create_engine, func, select)
from sqlalchemy.orm import Session, registry
from tqdm import tqdm

mapper_registry = registry()
Base = mapper_registry.generate_base()

class MixedTbl(Base):
    __tablename__ = 'mixed_tbl'
    
    id = Column(Integer,primary_key=True)
    age = Column(Integer,nullable=False)
    weight = Column(Float)
    name = Column(String(32),nullable=False)
    email = Column(String(256))
    
    money = Column(Numeric(10,4))
    desc = Column(Text())
    registed_at = Column(DateTime)
    last_login = Column(DateTime)
    summary = Column(String(2048))
    url = Column(Text)

db_url = ""
def get_engine():
    engine=create_engine(db_url,
                         fast_executemany=True,
                         pool_size=100,
                         query_cache_size=1000)
    return engine

def _prepare(count):
    engine = get_engine()
    with Session(engine) as session:
        objs = []
        for _ in range(count):
            mixed = MixedTbl(
                age= randrange(1,120,1),
                weight = random()*100,
                name = choice(ascii_lowercase)*32,
                email = choice(ascii_lowercase)*128,
                money = random()*10000,
                desc = choice(ascii_lowercase)*5120,
                registed_at = datetime.now(),
                last_login = datetime.now(),
                summary = choice(ascii_lowercase)*1024,
                url = choice(ascii_lowercase)*1024
            )
            objs.append(mixed)
        session.bulk_save_objects(objs)
        session.commit()
    engine.dispose()
    gc.collect()
    
def prepare(row_count):
    chunk = 5000
    
    '''
    pyodbc will meet memory leak problem when enable fast_executemany.
    so I had to run it in process to recycle memory
    '''
    loop = int(row_count/chunk)
    for _ in tqdm(range(loop)):
        p = Process(target=_prepare,args=(chunk,))
        p.start()
        p.join()
        
def burn_read_update(count):
    engine = get_engine()
    with Session(engine) as session:
        res = session.execute(select(func.max(MixedTbl.id)).select_from(MixedTbl))
        max_count = res.all()[0][0]
        for i in range(count):
            entity = session.execute(select(MixedTbl).filter_by(id=randint(1,max_count))).scalar_one()
            entity.name = 'update2'
            entity.money = random()*10000  
            entity.last_login = datetime.now()
            # entity.url = random.choice(letters)*512 # large then 595 length will slow down sql
            entity.url = ''.join(choices(ascii_lowercase,k=512))
            session.commit()
            
            
def burn(threads,count_per_thread):
    with ThreadPoolExecutor(max_workers=threads) as executor:
        for _ in range(threads):
            executor.submit(burn_read_update,count_per_thread)
            
            
def parse_argv():
    parser = argparse.ArgumentParser()
    parser.add_argument("command",choices=['prepare','burn','clear'],help='prepare: generate test data, burn: execute read and update, clear:delete test data')
    parser.add_argument('--threads',type=int,help='how many threads run during burn',default=5)
    parser.add_argument('--times',type=int,help='how many iteration run in each threads',default=200000)
    parser.add_argument('--rows',type=int,help='generate how many rows when prepare',default=500000)
    parser.add_argument('--db_url',type=str,required=True,help='database connection url')
    return parser.parse_args()
    
if __name__ == '__main__':
    args  = parse_argv()
    print("start")
    db_url = args.db_url
    engine = get_engine()
    Base.metadata.create_all(engine)
    
    if args.command=='prepare':
        prepare(args.rows)
        
    elif args.command=='burn':
        start = time.perf_counter()
        threads=args.threads
        counts = args.times
        burn(threads,counts)
        # burn_read_update(counts)
        time_used = time.perf_counter()-start
        per_second = threads*counts/time_used
        result = '''
times:{times}
execute seconds: {time_used:.2f}
times per second: {per_second:.2f}
        '''
        print(result.format(times=threads*counts,time_used=time_used,per_second=per_second))
        
    elif args.command=='clear':
        MixedTbl.__table__.drop(engine)
    
    