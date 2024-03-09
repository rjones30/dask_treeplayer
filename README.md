# dask_treeplayer
pyroot replacement for ROOT.gProof to allow parallel processing of compiled C++ TSelectors on a dask cluster

The ROOT (root.cern.ch) community has mostly completed the transition away from compiled user C++ classes to the use of RDataFrame queries to analyze data stored in ROOT trees. However, some of us are left with a collection of various TSelectors developed in the past, making use of certain efficient analysis patterms which are difficult or inefficent to recast in terms of elementary data transformations using RDataFrames. Configuration and operation of a PROOF cluster was fragile and error-prone, but it worked well once it was set up and running. Now that the ROOT developers have discontinued support for PROOF clusters, an alternative is needed that provides the same functionality, while avoiding the complexity of having to maintain a custom socket-level communications and exception handling rpc library like the one that PROOF employed in a security-conscious era.  This project provides just that, based on the dask futures API provided by the python dask module, while preserving the familiar client interface of TTreePlayer in pyroot.

To use this module, just copy dask_treeplayer.py into the PYTHONPATH that also incldues the pyroot libraries needed to import ROOT. One simple solution is just to place it in the local ROOT $ROOTSYS/lib directory. This ensures that any custom configuration applied to the header of dask_treeplayer.py during installation is associated with the build of ROOT on the local platform that it is designed to work with.

## Quick start
Place a copy of dask_treeplayer.py in your $ROOTSYS/lib directory, or somewhere else in your PYTHONPATH that also contains $ROOTSYS/lib. Open this copy of dask_treeplayer.py in a text editor and look at the values assigned to the following global configuration constants. 
```
# Check that these constants apply to the local cluster environment
ssh = "ssh"
workdir = os.getcwd()
daskdir = f"{workdir}/dask-worker-space"
rootlib = '/'.join(__file__.split('/')[:-1])
os.environ['LD_LIBRARY_PATH'] = f"{rootlib}:{os.environ['LD_LIBRARY_PATH']}"
os.environ['PYTHONPATH'] = f"{rootlib}:{os.environ['PYTHONPATH']}"
sys.path.insert(0, f"{rootlib}")

# select the local command to spawn the correct python3 for the ROOT build
python3 = "/usr/bin/python3"
#python3 = "/usr/bin/scl enable devtoolset-11 -- " + python3
```
Many of these will work in your environment without changes, but some customization is usually needed. For example, add whatever commandline options to the ssh string that are necessary to make password-less connections to your local cluster from your user account. The daskdir will be created at the start of a new dask_treeplayer session to contain log files and automatically generated scripts that facilitate the dask cluster communications. In case of errors, the logs in that directory may be useful to discover what went wrong with the execution of the user TSelector on the remote workers.

Start an interactive python3 session and import dask_treeplayer to make sure that all of the required dependencies are present and visible in your PYTHOBNPATH. Execute the following command for documentation on the features of dask_treeplayer, including an example pyroot session illustrating its use.
```
$ pydoc3 dask_treeplayer
```
