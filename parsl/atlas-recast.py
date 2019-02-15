import parsl.data_provider.files
from parsl import App
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.providers import KubernetesProvider

# Config for two kubernetes executors. One for each step, each with their own
# docker image. We also mount a shared volume into the steps so they can
# communicate with each other via a single file system.
config = Config(
    executors=[
        IPyParallelExecutor(
            label='event_selection',
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
            label='stat_analysis',
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

# event_selection task
@App('bash', executors=['event_selection'], cache=True)
def event_selection(did, name, xsec_in_pb, dxaod_file, submitDir, lumi_in_ifb,
                    stdout="/data/event_selection.out",
                    stderr="/data/event_selection.err",
                    outputs=[]):
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


# Statistical Analysis Task
@App('bash', executors=['stat_analysis'], cache=True)
def stat_analyis(data_file, signal_file, background_file, resultdir,
                 stdout="/data/stat_analyis.out",
                 stderr="/data/stat_analyis.err",
                 inputs=[], outputs=[]):
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

parsl.load(config)

sample_file = parsl.File("/data/submitDir/hist-sample.root")
post_file = parsl.File("/data/results//post.png")
event_selection_future = event_selection("404958", "recast_sample", 0.00122,
                                         dxaod_file="https://recastwww.web.cern.ch/recastwww/data/reana-recast-demo/mc15_13TeV.123456.cap_recast_demo_signal_one.root",
                                         submitDir="/data/submitDir",
                                         lumi_in_ifb=30.0,
                                         outputs=[sample_file])

stat_analysis_future = stat_analyis("/data/data.root", "/data/submitDir/hist-sample.root",
                           "/data/background.root", "/data/results",
                                    inputs=event_selection_future.outputs,
                                    outputs=[post_file])


# Check if the result file is ready
print ('Done: %s' % stat_analysis_future.outputs[0].done())
