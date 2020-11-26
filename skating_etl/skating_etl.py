#!/usr/bin/python3

def run(cloud=False):
    
#   imports
    
    import apache_beam as ab
    from apache_beam import io
    from apache_beam import ToString as ts
    from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions, GoogleCloudOptions
    
    from gcp_tools import beam_tools as bt
    from python_tools import scalar_functions as sf
    
#   field-level transform functions

    def forename(x): return x[sf.translate(x,'','abcdefghijklmnopqrstuvwxyz','x').find('x',x.find(' '))-1:].strip().title()
    def surname(x):  return x[:sf.translate(x,'','abcdefghijklmnopqrstuvwxyz','x').find('x',x.find(' '))-1].strip().title()
    def name1(x):    return x.split('/')[0].strip()
    def name2(x):    return (x + '/').split('/')[1].strip()
    def gender1(x):  return 'M' if x.title().find('Men ') >= 0 else 'F'
    def gender2(x):  return 'M' if x.find('/') >= 0 else ''     
    def program(x):  return x[0:x[::-1].replace(' ','|',2)[::-1].find('|')].rstrip('-').strip()
    def element(x):  return x[(x[::-1].replace(' ','|',2)[::-1].find('|'))+1:].strip()
    def asp_type(x): return x[0:1].upper()
    def str_int(x):  return '0' if x == '' else str(int(float(x)))
    def nullify(x):  return '' if x == '0' else x
 
#   set up pipeline arguments and variables

    shards = '-SSSSS-of-NNNNN' if cloud else ''
    path = 'gs://your-bucket/' if cloud else '/home/your_user_dir/'
    input_path = path + 'input' + '/'
    output_path = path + 'output' + '/'
    
    opt = PipelineOptions(flags=[])

    if cloud:
        opt.view_as(SetupOptions).save_main_session = True
        opt.view_as(SetupOptions).setup_file = './setup.py'
        opt.view_as(StandardOptions).runner = 'DataflowRunner'
        opt.view_as(GoogleCloudOptions).project = 'your-project'
        opt.view_as(GoogleCloudOptions).job_name = 'skating-etl'
        opt.view_as(GoogleCloudOptions).staging_location = path + 'temp'
        opt.view_as(GoogleCloudOptions).temp_location = path + 'temp'
        opt.view_as(GoogleCloudOptions).region = 'us-central1'

#   run the pipeline

    with ab.Pipeline(options=opt) as pipe:

#       extract the data

        p00 = (pipe | 'p00 Read Performance'        >> io.ReadFromText(input_path + 'performances.csv', skip_header_lines=1)
                    | 'p00 Switch Delimiter'        >> ab.ParDo(bt.SwitchDelimiters(',','|'))
                    | 'p00 ToList'                  >> ab.ParDo(bt.ConvertRecTo(list,'|')))

        p01 = (pipe | 'p01 Read Judges'             >> io.ReadFromText(input_path + 'judges.csv', skip_header_lines=1)
                    | 'p01 Switch Delimiter'        >> ab.ParDo(bt.SwitchDelimiters(',','|'))
                    | 'p01 ToList'                  >> ab.ParDo(bt.ConvertRecTo(list,'|')))

        p02 = (pipe | 'p02 Read Aspects'            >> io.ReadFromText(input_path + 'judged-aspects.csv', skip_header_lines=1)
                    | 'p02 Switch Delimiter'        >> ab.ParDo(bt.SwitchDelimiters(',','|'))
                    | 'p02 ToList'                  >> ab.ParDo(bt.ConvertRecTo(list,'|')))

        p03 = (pipe | 'p03 Read Scores'             >> io.ReadFromText(input_path + 'judge-scores.csv', skip_header_lines=1)
                    | 'p03 Switch Delimiter'        >> ab.ParDo(bt.SwitchDelimiters(',','|'))
                    | 'p03 ToList'                  >> ab.ParDo(bt.ConvertRecTo(list,'|')))

#       transform the data

        p10 = (p00 | 'p10 Events: Drop Fields'      >> bt.KeepFields(1,2,0)           # Keep: Comp, Prog/Element, Performance ID
                   | 'p10 Events: Distinct'         >> bt.DistinctList()
                   | 'p10 Events: Count'            >> bt.Count(0,1))                 # Outp: Comp, Prog/Element, Entries (Count(*))

        p15 = (p00 | 'p15 Perf: Split Skaters'      >> bt.XFormAppend(a3=name1,
                                                                      b3=name2,
                                                                      a2=gender1,
                                                                      c3=gender2))    # Add: Sk.Name 1 & 2, Sk.Gender 1 & 2
        

        p20 = (p15 | 'p20 Skaters: Drop Fields'     >> bt.KeepFields(4,11,12,13,14)   # Keep: Ctry, Sk.Name 1 & 2, Gender 1 & 2
                   | 'p20 Skaters: Parse Names'     >> bt.XFormAppend(a1=forename,
                                                                      b1=surname,
                                                                      a2=forename,
                                                                      b2=surname)     # Add: Sk.1 Fore & Surname, Sk.2 Fore & Surname
               
                   | 'p20 Skaters: Parse'           >> bt.Normalise(2,1,5,6,3,2,7,
                                                                    8,4,blanks='n')   # Outp: Ctry, Name, Forename, Surname, Gender
               
                   | 'p20 Skaters: Rearrange'       >> bt.KeepFields(1,2,3,4,0)       # Outp: Name, Forename, Surname, Gender, Ctry
                   | 'p20 Skaters: Distinct'        >> bt.DistinctList())             
        

        p25 = (p01 | 'p25 Judges: Drop Fields'      >> bt.KeepFields(2,2,2,'x',7)     # Keep: Name, Name, Name, Null, Ctry
                   | 'p25 Judges: Distinct'         >> bt.DistinctList()              
                   | 'p25 Judges: Parse Names'      >> bt.XForm(t1=forename,
                                                                t2=surname))          # Outp: Name, Forename, Surname, Null, Ctry
        
   
        p30 = ((p20, p25)
                   | 'p30 Person: Combine'          >> ab.Flatten()                   # Combine: Skaters & Judges
                   | 'p30 Person: Sort'             >> bt.Sort(2,1,4,3)               # Sort: Surname, Forename, Ctry, Gender
                   | 'p30 Person: Generate SKs'     >> bt.GenerateSKs())              # Outp: PersonID, Name, Fore & Surname, Gender, Ctry
        
        p35 = (p01 | 'p35 Events: Drop Fields'      >> bt.KeepFields(5,4)             # Keep: Comp, Prog/Element
                   | 'p35 Events: Distinct'         >> bt.DistinctList()               
                   | 'p35 Events: Sort'             >> bt.Sort(0,1)                   # Sort: Comp, Prog/Element
                   | 'p35 Events: Add Entries'      >> bt.Join(p10,'Left',
                                                               key='0,1',
                                                               keep='0,1,4')          # Outp: Comp, Prog/Element, Entries
                   | 'p35 Events: Generate SKs'     >> bt.GenerateSKs())              # Outp: EventID, Comp, Prog/Element, Entries
        

        p40 = (p01 | 'p40 J.Roles: Drop Fields'     >> bt.KeepFields(5,2,4,6)         # Keep: Comp, Name, Prog/Element, Role
                   | 'p40 J.Roles: Add Person FK'   >> bt.Join(p30,'Left',
                                                               key=1,
                                                               keep='3,0,2,4')        # Outp: Role, Comp, Prog/Element, PersonID
                   | 'p40 J.Roles: Add Events FK'   >> bt.Join(p35,'Left',
                                                               key='1,2',
                                                               keep='0,3,4'))         # Outp: Role, PersonID, EventID         

        p45 = (p15 | 'p45 Perf: Drop Fields'        >> bt.DropFields(3,4,7,13,14)     # Keep: PerfID, Comp, Prog/Elm, Rank, Seq, El.Score
                                                                                      #       Co.Score, Ded, Sk.1 Name, Sk.2 Name
                   | 'p45 Perf: Add Events FK'      >> bt.Lookup(p35,'Left',
                                                          side_val=0,
                                                          key='1,2',                  # Outp: PerfID, Rank, Seq, El.Score, Co.Score, Ded,   
                                                          keep='0,3,4,5,6,7,8,9,10')  #       Sk.1 Name, Sk.2 Name, EventID           
                   | 'p45 Perf: Add Skater 1 FK'    >> bt.Lookup(p30,'Left',
                                                          side_val=0,
                                                          main_key=6,side_key=1,            
                                                          keep='0,1,2,3,4,5,7,8,9')
                   | 'p45 Perf: Add Skater 2 FK'    >> bt.Lookup(p30,'Left',
                                                          side_val=0,
                                                          main_key=6,side_key=1,      # Outp: PerfID, Rank, Seq, El.Score, Co.Score, Ded,         
                                                          keep='0,1,2,3,4,5,7,8,9')   #       EventID, Sk.1 ID, Sk.2 ID   
                   | 'p45 Perf: Distinct'           >> bt.DistinctList()              
                   | 'p45 Perf: Sort'               >> bt.Sort(6.,2.,0)               # Sort: EventID, Seq, PerfID
                   | 'p45 Perf: Generate SKs'       >> bt.GenerateSKs())              # Outp: PerformanceID, PerfID, Rank, Seq, El.Score,
                                                                                      #       Co.Score, Ded, EventID, Sk.1 ID, Sk.2 ID
               

        p50 = (p02 | 'p50 J.Aspect: Drop Fields'    >> bt.KeepFields(0,1,2,4,3,7,11)  # Keep: J.AspectID, PerfID, Type, Desc, Seq,
                                                                                      #       B.Diff, Score
                   | 'p50 J.Aspect: Distinct'       >> bt.DistinctList()               
                   | 'p50 J.Aspect: Transform'      >> bt.XForm(t2=asp_type,
                                                                t4=str_int))
        

        p55 = (p50 | 'p55 Aspect: Drop Fields'      >> bt.KeepFields(2,3)             # Keep: Type, Desc
                   | 'p55 Aspect: Distinct'         >> bt.DistinctList()               
                   | 'p55 Aspect: Sort'             >> bt.Sort(0,1)                   # Sort: Type, Desc
                   | 'p55 Aspect: Generate SKs'     >> bt.GenerateSKs())              # Outp: AspectID, Aspect Type, Aspect Desc
        
     
        p60 = (p50 | 'p60 J.Aspect: Apply Perf FK'  >> bt.Lookup(p45,'Left',          # Keep: J.AspectID, Type, Desc, Seq, B.Diff,      
                                                          key=1,side_val=0,           #       Score, PerformanceID
                                                          keep='0,2,3,4,5,6,7')
               
                   | 'p60 J.Aspect: Apply Asp. FK'  >> bt.Lookup(p55,'Left',          # Keep: J.AspectID, Seq, B.Diff, Score      
                                                          key='1,2',side_val=0,       #       PerformanceID, AspectID
                                                          keep='0,3,4,5,6,7') 

                   | 'p60 J.Aspect: Sort'           >> bt.Sort(4.,1.,5.,0)            # Sort: PerformanceID, Seq, AspectID, J.AspectID
                   | 'p60 J.Aspect: XForm Seq'      >> bt.XForm(t1=nullify)           
                   | 'p60 J.Aspect: Generate SKs'   >> bt.GenerateSKs())              # Outp: JudgedAspectID, J.AspectID, Seq, B.Diff,
                                                                                      #       Score, PerformanceID, AspectID

        p65 = (p03 | 'p65 Scores: Distinct'         >> bt.DistinctList()
                   | 'p65 Scores: Add J.Aspect ID'  >> bt.Lookup(p60,'Left',          # Outp: Role, Score, JudgedAspectID
                                                          side_key=1,side_val=0,
                                                          main_key=0,
                                                          keep='1,2,3')                
                   | 'p65 Scores: Add Perf. ID'     >> bt.Lookup(p60,'Left',          # Outp: Role, Score, JudgedAspectID, PerformanceID
                                                          side_key=0,side_val=5,
                                                          main_key=2,
                                                          keep='0,1,2,3')                 
                   | 'p65 Scores: Add Event. ID'    >> bt.Lookup(p45,'Left',          # Outp: Role, Score, JudgedAspectID, EventID
                                                          side_key=0,side_val=7,
                                                          main_key=3,
                                                          keep='0,1,2,4')                
                   | 'p65 Scores: Add Person ID'    >> bt.Lookup(p40,'Left',
                                                          side_key='2,0',side_val=1,
                                                          main_key='3,0',
                                                          keep='2,4,1')               # Outp: JudgedAspectID, PersonID, Score    
                   | 'p65 Scores: Sort'             >> bt.Sort(0.,1.)                 # Sort: JudgedAspectID, PersonID
                   | 'p65 Scores: Generate SKs'     >> bt.GenerateSKs())              # Outp: ScoreID, JudgedAspectID, PersonID, Score

#       load the data

        p91 = (p30 | 'p91 Person: Reformat'         >> bt.DropFields(1)               # Outp: PersonID, Forename, Surname, Gender, Ctry
                   | 'p91 Person: ToStr'            >> ts.Iterables(delimiter='|')       
                   | 'p91 Person: Write File '      >> io.WriteToText
                                                             (output_path + 'person.dat',
                                                              shard_name_template=shards))

        p92 = (p35 | 'p92 Event: Dupe Prog/Elem'    >> bt.KeepFields(0,1,2,2,3)       # Outp: EventID, Comp, Prog/Elm, Prog/Elm, Entries
                   | 'p92 Event: Parse Prog/Elem'   >> bt.XForm(t2=program,
                                                                t3=element)           # Outp: EventID, Comp, Program, Element, Entries
                   | 'p92 Event: ToStr'             >> ts.Iterables(delimiter='|')
                   | 'p92 Event: Write File'        >> io.WriteToText
                                                             (output_path + 'event.dat',
                                                              shard_name_template=shards))

        p93 = (p45 | 'p93 Perf: Reformat'           >> bt.KeepFields(0,7,3,8,9,       # Outp: PerformanceID, EventID, Seq, Sk1 ID, Sk2 ID
                                                                     2,5,4,6)         #       Rank, Co.Score, El.Score, Ded
                   | 'p93 Perf: ToStr'              >> ts.Iterables(delimiter='|')
                   | 'p93 Perf: Write File'         >> io.WriteToText
                                                             (output_path + 'performance.dat',
                                                              shard_name_template=shards))

        p94 = (p55 | 'p94 Aspect: ToStr'            >> ts.Iterables(delimiter='|')
                   | 'p94 Aspect: Write File'       >> io.WriteToText
                                                             (output_path + 'aspect.dat',
                                                              shard_name_template=shards))

        p95 = (p60 | 'p95 J.Aspect: Reformat'       >> bt.KeepFields(0,5,2,6,3,4)     # Outp: J.Asp.ID, Perf.ID, Seq, AspID, B.Diff, Score
                   | 'p95 J.Aspect: ToStr'          >> ts.Iterables(delimiter='|')
                   | 'p95 J.Aspect: Write File'     >> io.WriteToText
                                                             (output_path + 'performance_aspect.dat',
                                                              shard_name_template=shards))

        p96 = (p65 | 'p96 Scores: ToStr'            >> ts.Iterables(delimiter='|')
                   | 'p96 Scores: Write_File'       >> io.WriteToText
                                                             (output_path + 'performance_scores.dat',
                                                              shard_name_template=shards))

if __name__ == '__main__': run()
