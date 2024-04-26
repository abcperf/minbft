# MinBFT

MinBFT is a Rust implementation based on the equally named Byzantine fault-tolerant 
consensus algorithm presented in the paper [Efficient Byzantine Fault-Tolerance](https://doi.org/10.1109/TC.2011.221).

MinBFT requires a Unique Sequential Identifier Generator (USIG) implementation.\
A USIG implementation compatible with this MinBFT implementation can be found 
[here](https://github.com/abcperf/usig).
Note that this implementation does not use Trusted Execution Environments and, 
thus, should not be used in untrusted environments.

This implementation was created as part of the [ABCperf project](https://doi.org/10.1145/3626564.3629101).\
An [integration in ABCperf](https://github.com/abcperf/demo) also exists.

# MinBFT in Action

```rs
use anyhow::Result;
use std::{num::NonZeroU64, time::Duration};
use serde::{Deserialize, Serialize};
use shared_ids::{ReplicaId, ClientId, RequestId};
use usig::{Usig, noop::UsigNoOp};
use minbft::{MinBft, Config, Output, RequestPayload, PeerMessage, timeout::{TimeoutType}};

// The payload of a client request must be implemented by the user.
#[derive(Deserialize, Serialize, Clone, Debug, Eq, PartialEq)]
struct SamplePayload {}
impl RequestPayload for SamplePayload {
    fn id(&self) -> RequestId {
        todo!()
    }
    fn verify(&self, id: ClientId) -> Result<()> {
        todo!()
    }
}

// The output should be handled by the user.
fn handle_output<U: Usig>(output: Output<SamplePayload, U>) {
    let Output { broadcasts, responses, timeout_requests, .. } = output;
    for broadcast in broadcasts.iter() {
        // broadcast message to all peers, i.e., send messages contained
        // in `broadcasts` of struct Output over the network to other
        // replicas.
        todo!();
    }
    for response in responses.iter() {
        todo!();
    }
    for timeout_request in timeout_requests.iter() {
        todo!();
    }
}

fn main() {
    // define the amount of replicas that form the atomic broadcast
    let n = NonZeroU64::new(10).unwrap();
    // define the maximum amount of faulty replicas (`n >= t / 2 + 1`)
    let t = n.get() / 2;
    // define the ID of one of the replicas
    let replica_id = ReplicaId::from_u64(0);

    let config = Config {
                n: n.try_into().unwrap(),
                t,
                id: replica_id,
                max_batch_size: Some(1.try_into().expect("> 0")),
                batch_timeout: Duration::from_secs(1),
                initial_timeout_duration: Duration::from_secs(1),
                checkpoint_period: NonZeroU64::new(2).unwrap(),
            };
    
    // can be exchanged with a different USIG implementation
    let usig = UsigNoOp::default();

    let (mut minbft, output) = MinBft::<SamplePayload, _>::new(
            usig,
            config,
        )
        .unwrap();

    // handle output to setup connection with peers
    handle_output(output);

    // all peers are now ready for client requests as they have
    // successfully communicated with each replica for the first time.

    // create and send client message to be handled by replicas.
    let some_client_message: SamplePayload = todo!();
    let output = minbft.handle_client_message(ClientId::from_u64(0), some_client_message);
    handle_output(output);
    // after the atomic broadcast achieves consensus,
    // a response to the client request can be seen in the output struct.
}
```

## Contact

Feel free to reach out via [GitHub](https://github.com/abcperf/minbft), 
[matrix](https://matrix.to/#/@vo5598:kit.edu), or [mail](mailto:marc.leinweber@kit.edu).

## License

Licensed under [MIT license](https://github.com/abcperf/minbft/blob/main/LICENSE).
