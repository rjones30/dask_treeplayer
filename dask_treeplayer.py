#!/usr/bin/scl enable devtoolset-11 -- python3
#
# dask_treeplayer.py - python module to emulate the behavior of
#                      the ROOT proof server using the python
#                      dask multiprocess scheduler.
#
# author: richard.t.jones at uconn.edu
# version: february 29, 2024
#
# example usage:
#
#   #!/usr/bin/env python3
#   import os
#   import ROOT
#   import dask_treeplayer
#   import socket
#   import subprocess
#   
#   tchain = ROOT.TChain("mytree", "")
#   for itree in range(14):
#      tchain.Add(f"root://xrootdsrv.my.net/data/access/path/dtree_{i}.root")
#   
#   workers = {"worker1.my.net": 12,
#              "worker2.my.net": 12,
#              "worker3.my.net": 14,
#              "worker4.my.net": 24}
#
#   with dask_treeplayer.session(tchain).spawn(workers) as session:
#      print("inside session")
#      rc = session.Process("moller.C+g", "6", chunksize=1000000)
#      for result in session.GetOutputList():
#         print(f"{result.GetName()} has {result.GetEntries()} entries")
#   print("outside session")

import os
import sys
import time
import subprocess
import socket
import signal
import re

# Check that these constants apply to the local cluster environment
ssh = "ssh"
workdir = os.getcwd()
daskdir = f"{workdir}/dask-worker-space"
dask_worker_comm_timeout = 300 # seconds
rootlib = '/'.join(__file__.split('/')[:-1])
os.environ['LD_LIBRARY_PATH'] = f"{rootlib}:{os.environ['LD_LIBRARY_PATH']}"
os.environ['PYTHONPATH'] = f"{rootlib}:{os.environ['PYTHONPATH']}"
sys.path.insert(0, f"{rootlib}")

# select the local command to spawn the correct python3 for the ROOT build
python3 = "/usr/bin/python3"
python3 = "/usr/bin/scl enable devtoolset-11 -- " + python3

import cloudpickle as pickle
import ROOT
import dask
import dask.distributed

dask.config.set({"distributed.comm.timeouts.connect": f"{dask_worker_comm_timeout}s"})

client = 0

class session:
   """
   Represents a dask cluster consisting of a scheduler running on the
   same host as the client, and a collection of workers running on a
   cluster that mounts the user's working directory at the same path
   as the client. Access to all of the root files pointed to by the
   ROOT.TChain object passed to the constructor is assumed to exist
   on both client and workers at the same pathnames. Startup of the
   dask scheduler and worker processes is triggered by session.spawn()
   where the user specifies the list of cluster worker nodenames and
   the number of threads to run on each. Only one spawn may be active
   at any given time on a session.
   """
   def __init__(self, chain):
      """
      Initializes a dask_treeplayer session for distributed processing
      of a ROOT.TChain on a cluster using the dask futures interface,
      where
         chain = (ROOT.TChain) the input root tree to be processed.
      Normally user code would call this constructor at the entry to
      a new context, to properly manage the lifetime of the session.
      """
      self.chain = chain
      self.workers = {}
      self.spawned = {}
      self.procs = {}
      self.port = 0

   def spawn(self, workers, port=8768):
      """
      Starts a new dask scheduler and remote set of workers to handle
      subsequent processing of tree queries using the user TSelector
      declared for this session. Input arguments are
         * workers = (dict) collection of dask workers in the form
                       {"<hostname>": <nprocess>, ...}
         * port = (int) port number used by dask scheduler
      Return value is the session object, normally used to establish
      a new context to properly manage the lifetime of the dask
      scheduler and workers.
      """
      if self.port != 0:
         raise Exception("error - session already spawned on port {port}")
      self.workers = workers
      self.port = port
      return self

   def __enter__(self):
      if self.port == 0:
         raise Exception("error - use session.spawn to open a new query context")
      worker_script = self.__new_worker_script()
      scheduler_script = self.__new_scheduler_script()
      client_hostname = socket.gethostname()
      client_ipaddr = socket.gethostbyname(client_hostname)
      self.client_url = f"tcp://{client_ipaddr}:{self.port}"
      if not os.path.isdir(daskdir):
         os.mkdir(daskdir)
      scmd = [f"{scheduler_script}", "--port", f"{self.port}"]
      stdout = open(f"{scheduler_script}.out", "w")
      stderr = open(f"{scheduler_script}.err", "w")
      self.spawned = subprocess.Popen(scmd, stdout=stdout, stderr=stderr,
                                      preexec_fn=os.setsid)
      nthreads = 1
      self.procs = {}
      for worker in self.workers:
         for iproc in range(self.workers[worker]):
            wcmd = ssh.split(' ') + [
                    f"{worker}", f"{worker_script}",
                    f"--nthreads", f"{nthreads}",
                    f"tcp://{client_ipaddr}:{self.port}"]
            stdout = open(f"{worker_script}.{len(self.procs)+1}.out", "w")
            stderr = open(f"{worker_script}.{len(self.procs)+1}.err", "w")
            self.procs[f"{worker}:{iproc}"] = subprocess.Popen(wcmd,
                                              stdout=stdout, stderr=stderr,
                                              preexec_fn=os.setsid)
      self.client = dask.distributed.Client(self.client_url)
      return self

   def __exit__(self, exc_type, exc_val, exc_tb):
      if self.port == 0:
         return 0
      os.killpg(os.getpgid(self.spawned.pid), signal.SIGTERM)
      for worker in self.procs:
         os.killpg(os.getpgid(self.procs[worker].pid), signal.SIGTERM)
 
   def __new_worker_script(self):
      name = self.chain.GetName()
      title = self.chain.GetTitle()
      sname = f"{daskdir}/dask-worker-{name}.{self.port}"
      workscript = open(sname, "w")
      workscript.write(f"#!{python3}\n")
      workscript.write("\n")
      workscript.write("import os\n")
      workscript.write("import sys\n")
      workscript.write("import re\n")
      workscript.write("\n")
      workscript.write(f"rootlib = '{rootlib}'\n")
      workscript.write("os.environ['LD_LIBRARY_PATH'] = f\"{rootlib}:{os.environ['LD_LIBRARY_PATH']}\"\n")
      workscript.write("os.environ['PYTHONPATH'] = f\"{rootlib}:{os.environ['PYTHONPATH']}\"\n")
      workscript.write("sys.path.insert(0, f\"{rootlib}\")\n")
      workscript.write("\n")
      workscript.write("import cloudpickle as pickle\n")
      workscript.write("import ROOT\n")
      workscript.write("\n")
      workscript.write("import dask\n")
      workscript.write(f"dask.config.set({{\"distributed.comm.timeouts.connect\": f\"{dask_worker_comm_timeout}s\"}})\n")
      workscript.write("from distributed.cli.dask_worker import go\n")
      workscript.write("\n")
      workscript.write(f"input_chain = ROOT.TChain('{name}', '{title}')\n")
      workscript.write("\n")
      for chel in self.chain.GetListOfFiles():
         workscript.write(f"input_chain.Add('{chel.GetTitle()}')\n")
      workscript.write("\n")
      workscript.write("if __name__ == '__main__':\n")
      workscript.write("   sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])\n")
      workscript.write("   sys.exit(go())\n")
      workscript.close()
      os.chmod(sname, 0o755)
      return sname

   def __new_scheduler_script(self):
      name = self.chain.GetName()
      title = self.chain.GetTitle()
      sname = f"{daskdir}/dask-scheduler-{name}.{self.port}"
      workscript = open(sname, "w")
      workscript.write(f"#!{python3}\n")
      workscript.write("\n")
      workscript.write("import os\n")
      workscript.write("import sys\n")
      workscript.write("import re\n")
      workscript.write("\n")
      workscript.write(f"rootlib = '{rootlib}'\n")
      workscript.write("os.environ['LD_LIBRARY_PATH'] = f\"{rootlib}:{os.environ['LD_LIBRARY_PATH']}\"\n")
      workscript.write("os.environ['PYTHONPATH'] = f\"{rootlib}:{os.environ['PYTHONPATH']}\"\n")
      workscript.write("sys.path.insert(0, f\"{rootlib}\")\n")
      workscript.write("\n")
      workscript.write("import cloudpickle as pickle\n")
      workscript.write("import ROOT\n")
      workscript.write("\n")
      workscript.write("import dask\n")
      workscript.write(f"dask.config.set({{\"distributed.comm.timeouts.connect\": f\"{dask_worker_comm_timeout}s\"}})\n")
      workscript.write("from distributed.cli.dask_scheduler import go\n")
      workscript.write("\n")
      workscript.write(f"input_chain = ROOT.TChain('{name}', '{title}')\n")
      workscript.write("\n")
      for chel in self.chain.GetListOfFiles():
         workscript.write(f"input_chain.Add('{chel.GetTitle()}')\n")
      workscript.write("\n")
      workscript.write("if __name__ == '__main__':\n")
      workscript.write("   sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])\n")
      workscript.write("   sys.exit(go())")
      workscript.close()
      os.chmod(sname, 0o755)
      return sname

   def __wrap_selector(self, selector):
      muser = re.match(r"(.*)\.[^\.]*$", selector)
      if not muser or not os.path.isfile(muser.group(1) + ".h"):
         print(f"Error - cannot find header file for TSelector {selector}")
         print(f"Try again with the full name of the source, eg. myselector.C")
         return 0
      msource = re.match(r"(.*\.[^\+])[^\.]*$", selector)
      if not msource or not os.path.isfile(msource.group(1)):
         print(f"Error - cannot find source file for TSelector {selector}")
         print(f"Try again with the full name of the source, eg. myselector.C")
         return 0
      userclass = muser.group(1)
      usersource = msource.group(1)
      userheader = userclass + ".h"
      wrapheader = f"dask_{userclass}.h"
      wrapsource = f"dask_{userclass}.C"
      if os.path.isfile(wrapsource) and os.path.isfile(wrapsource) and\
         os.path.getmtime(wrapsource) > os.path.getmtime(usersource) and\
         os.path.getmtime(wrapheader) > os.path.getmtime(userheader)\
      :
         return f"dask_{selector}"
      with open(wrapheader, "w") as hdr:
         hdr.write(
          f"// This header was generated automatically by dask_treeplayer\n" +
          f"// to make a thin wrapper around user TSelector {userclass}. It\n" +
          f"// will be automatically overwritten, DO NOT EDIT by hand.\n" +
          f"\n" +
          f"#undef {userclass}_cxx\n" +
          f'#include "{userheader}"\n' +
          f"\n" +
          f"class dask_{userclass}: public {userclass} {{\n" +
          f" public:\n" +
          f"   virtual void Terminate() {{}}\n" +
          f"\n" +
          f"   ClassDef(dask_{userclass},0);\n" +
          f"}};\n")
      with open(wrapsource, "w") as src:
         src.write(
          f"// This source file was generated automatically by dask_treeplayer\n" +
          f"// to make a thin wrapper around user TSelector {userclass}. It\n" +
          f"// will be automatically overwritten, DO NOT EDIT by hand.\n" +
          f"\n" +
          f'#include "{wrapheader}"\n')
      return f"dask_{selector}"

   def __load_selector(self, selector, timeout=60):
      rc = ROOT.gROOT.ProcessLine(f".L {selector}")
      if rc != 0:
         print(f"error {rc} loading selector {selector} on client")
         return rc
      wrapselector = self.__wrap_selector(selector)
      rc = ROOT.gROOT.ProcessLine(f".L {wrapselector}")
      if rc != 0:
         print(f"error {rc} loading selector {wrapselector} on client")
         return rc
      dask_worker_timer = 0
      while len(self.client.scheduler_info()['workers']) < len(self.procs):
         time.sleep(1)
         dask_worker_timer += 1
         print(f"connecting to workers... {dask_worker_timer}s", end='\r', flush=True)
         if dask_worker_timer == timeout:
            print("timeout waiting for all workers to start,",
                  "getting started anyway...")
            return -1
      print(f"connecting to workers... {dask_worker_timer}s done")
      treename = self.chain.GetName()
      def remote_load_selector(selector):
         oldpath = ROOT.gROOT.GetMacroPath()
         ROOT.gROOT.SetMacroPath(f"{oldpath}:{workdir}")
         return ROOT.gROOT.ProcessLine(f".L {selector}")
      errors = self.client.run(remote_load_selector, selector)
      for worker in errors:
         if errors[worker] != 0:
            print(f"error {errors[worker]} loading {selector} on {worker}")
            return errors[worker]
      errors = self.client.run(remote_load_selector, wrapselector)
      for worker in errors:
         if errors[worker] != 0:
            print(f"error {errors[worker]} loading {wrapselector} on {worker}")
            return errors[worker]
      return 0

   def Process(self, selector, option="", nentries=-1, firstentry=0, chunksize=100000):
      """
      Designed to function as a drop-in replacement ROOT.gProof.Process()
      that oversees remote execution of the following members of a user
      C++ class over selected rows in a ROOT.TChain found on a shared
      filesystem or on a remote xrootd server. The user TSelector must
      be customized to access the data in the specific TChain it will
      be called to analyze, starting from a C++ template generated by
      TChain.MakeSelector("myclass"). The Input arguments are
        * selector =   (string) the name of the C++ source file containing
                       the user TSelector, with an optional suffix that
                       specifies how it should be compiled; see the ROOT
                       documentation on TTree.Process(selector).
        * option =     (string) user-defined argument passed through to
                       the TSelector methods.
        * nentries =   (int) number of rows of the TChain to loop over.
        * firstentry = (int) row of the TChain where processing starts.
        * chunksize =  (int) number of rows of the TChain to process at
                       a time, before saving intermediate results.
      Return value is zero if processing completed without errors.
      """
      muser = re.match(r"^(.*)\.[^\.]*$", selector)
      if muser:
         self.__load_selector(selector)
         userclass = muser.group(1)
      else:
         userclass = selector
      wrapclass = f"dask_{userclass}"
      treename = self.chain.GetName()

      def process_chunk(opt, nentries, firstentry):
         ROOT.gROOT.ProcessLine(f"usersel = new {wrapclass}();")
         try:
            ROOT.usersel
         except:
            oldpath = ROOT.gROOT.GetMacroPath()
            if not f":{workdir}" in oldpath:
               ROOT.gROOT.SetMacroPath(f"{oldpath}:{workdir}")
            ROOT.gROOT.ProcessLine(f".L {selector}")
            ROOT.gROOT.ProcessLine(f".L dask_{selector}")
            ROOT.gROOT.ProcessLine(f"usersel = new {wrapclass}();")
         try:
            ROOT.gROOT.ProcessLine(f"userchain = (TChain*)gROOT->FindObject(" +
                                   f"\"{treename}\");")
            nseen = ROOT.userchain.Process(ROOT.usersel, opt, nentries, firstentry)
            result = ROOT.usersel.GetOutputList()
         except:
            return (0, ROOT.TList())
         return (nseen, result)

      def sum_chunks(outputslist):
         nseen = sum([outputslist[i][0] for i in range(len(outputslist))])
         results = outputslist[0][1]
         for outputs in outputslist[1:]:
            for output in outputs[1]:
               result = results.FindObject(output.GetName())
               result.Add(output)
         return (nseen, results)

      def pairwise_sums(subchunks):
         n = len(subchunks)
         return [self.client.submit(sum_chunks, subchunks[i:min(n,i+2)])
                 for i in range(0, n, 2)]

      if nentries == -1:
         nentries = self.chain.GetEntries()
      print(f"Processing {treename}: nentries={nentries},",
            f"chunksize={chunksize}, firstentry={firstentry}")
      print(f"Monitor progress at", self.client.dashboard_link)
      if nentries == 0:
         return ROOT.Tlist()

      chunks = [self.client.submit(process_chunk, option, chunksize, row)
                for row in range(firstentry, firstentry + nentries, chunksize)]
      sum_cascade = []
      sum_cascade.append(chunks)
      while len(sum_cascade[-1]) > 1:
         sum_cascade.append(pairwise_sums(sum_cascade[-1]))

      try:
         self.results = sum_cascade[-1][0].result()
      except KeyboardInterrupt:
         print("Processing interrupted by user, query canceled")
         return 15

      ROOT.gROOT.ProcessLine(f"usersel = new {userclass};")
      for result in self.results[1]:
         ROOT.usersel.GetOutputList().Add(result)
      ROOT.usersel.Terminate()
      return self.results[0]

   def GetOutputList(self):
      """
      Return the results of ROOT.TSelector.GetOutputList from the last
      Process query called on this session. This provides a quick way to
      access the results of a Process query without having to save the
      summed objects in a ROOT file in the TSelector.Terminate() method
      and then read them from the ROOT file, as was required in the 
      original gProof.Process implementation. The original method using
      a ROOT file to save the output objects still works as well.
      """
      try:
         outputlist = ROOT.usersel.GetOutputList()
      except:
         outputlist = ROOT.TList()
      return outputlist
