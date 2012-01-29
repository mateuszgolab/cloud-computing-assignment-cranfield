package uk.ac.cranfield.cloudcomputing.assignment.worker;






public class Main
{
    
    public static final String DATA_QUEUE = "matDataQueue";
    public static final String RESULT_QUEUE = "matResultQueue";
    private static String workerQueue;



    public static void main(String[] args)
    {
        if (args == null || args.length < 1)
            return;
        workerQueue = args[0] + "workerQueue";


        Worker worker = new Worker(DATA_QUEUE, RESULT_QUEUE, workerQueue);
        worker.connectQueues();

        while (true)
        {
            switch (worker.receiveStartingMessage()) {
                case ADDITION:
                    break;
                case MULTIPLICATION:
                    break;
                case END:
                    return;
            }
        }


    }
}
