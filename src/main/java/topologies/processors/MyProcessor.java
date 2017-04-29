package topologies.processors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Custom Streams processor
 */
public class MyProcessor implements Processor<String, String> {
    private ProcessorContext context;
    private KeyValueStore<String, Long> kvStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        // keep the processor context locally because we need it in punctuate() and commit()
        this.context = context;

        // call this processor's punctuate() method every 1000 milliseconds.
        this.context.schedule(1000);

        // retrieve the key-value store named "Counts"
        this.kvStore = (KeyValueStore<String, Long>) context.getStateStore("Counts");
    }

    /*
        Used on every record
     */
    @Override
    public void process(String dummy, String line) {
        String[] words = line.toLowerCase().split(" ");

        for (String word : words) {
            Long oldValue = this.kvStore.get(word);
            // store state
            if (oldValue == null) {
                this.kvStore.put(word, 1L);
            } else {
                this.kvStore.put(word, oldValue + 1L);
            }
        }
    }

    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, Long> iter = this.kvStore.all();
        // adjust punctuate interval
        //context.schedule(100);

        while (iter.hasNext()) {
            KeyValue<String, Long> entry = iter.next();
            // send down stream
            context.forward(entry.key, entry.value.toString());
        }

        iter.close();
        // commit the current processing progress
        context.commit();
    }

    @Override
    public void close() {
        // close any resources managed by this processor.
        // Note: Do not close any StateStores as these are managed
        // by the library
    }
};