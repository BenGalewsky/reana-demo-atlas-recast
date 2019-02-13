from parsl import App, DataFlowKernel
from parsl.app.app import bash_app, python_app
# from parsl.configs.local_threads import config

from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.providers import KubernetesProvider
from parsl.data_provider.files import File
import os.path

config = Config(
    executors=[
        IPyParallelExecutor(
            label='pool_app1',
            provider=KubernetesProvider(
                image="bengal1/reana-demo-atlas-recast-eventselection:latest",
                nodes_per_block=1,
                init_blocks=1,
                max_blocks=1,
                parallelism=1,
                persistent_volumes=[("task-pv-claim","/data")]
            )
        ),
        IPyParallelExecutor(
            label='pool_app2',
            provider=KubernetesProvider(
                image="bengal1/reana-demo-atlas-recast-statanalysis",
                nodes_per_block=1,
                init_blocks=1,
                max_blocks=1,
                parallelism=1,
                persistent_volumes=[("task-pv-claim", "/data")]
            )
        )
    ],
    lazy_errors=False
)

dfk = DataFlowKernel(config=config)


@App('bash', dfk, executors=['pool_app1'], cache=True)
def app_2(did, name, xsec_in_pb, dxaod_file, submitDir, lumi_in_ifb, stdout="/data/app2.out", stderr="/data/app2.err"):
    return '''
source /home/atlas/release_setup.sh
source /analysis/build/x86*/setup.sh
cat << 'EOF' > recast_xsecs.txt
      id/I:name/C:xsec/F:kfac/F:eff/F:relunc/F
      %s %s %f 1.0 1.0 1.0
EOF
cat recast_xsecs.txt
echo %s > recast_inputs.txt
myEventSelection %s recast_inputs.txt recast_xsecs.txt %f
        ''' % (did, name, xsec_in_pb, dxaod_file, submitDir, lumi_in_ifb)


@App('bash', dfk, executors=['pool_app2'], cache=True)
def app_3(data_file, signal_file, background_file, resultdir, stdout="/data/app3.out", stderr="/data/app3.err" ):
    return  '''
source /home/atlas/release_setup.sh
python /data/code/make_ws.py %(data_file)s %(signal_file)s %(background_file)s
mv ./results %(result_dir)s
python /data/code/plot.py %(result_dir)s/meas_combined_meas_model.root %(result_dir)s/pre.png %(result_dir)s/post.png
python /data/code/set_limit.py %(result_dir)s/meas_combined_meas_model.root \
     %(result_dir)s/limit.png %(result_dir)s/limit_data.json \
     %(result_dir)s/limit_data_nomsignal.json
        ''' % {
        "data_file": data_file,
        "signal_file": signal_file,
        "background_file": background_file,
        "result_dir": resultdir
    }

app_future = app_2("404958", "recast_sample", 0.00122,
                   dxaod_file="https://recastwww.web.cern.ch/recastwww/data/reana-recast-demo/mc15_13TeV.123456.cap_recast_demo_signal_one.root",
                   submitDir="/data/submitDir",
                   lumi_in_ifb=30.0                   )



# Check if the app_future is resolved
print ('Done1: %s' % app_future.done())

# Print the result of the app_future. Note: this
# call will block and wait for the future to resolve
print ('Result1: %s' % app_future.result())

app_future2 = app_3("/data/data.root", "/data/submitDir/hist-sample.root", "/data/background.root", "/data/results")
print ('Done2: %s' % app_future2.done())

print ('Result2: %s' % app_future2.result())
print ('Done: %s' % app_future.done())
