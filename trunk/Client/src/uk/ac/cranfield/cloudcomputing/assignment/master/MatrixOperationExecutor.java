package uk.ac.cranfield.cloudcomputing.assignment.master;

import java.util.List;

import javax.swing.SwingWorker;


public abstract class MatrixOperationExecutor extends SwingWorker<Long, Integer>
{
    
    @Override
    protected abstract Long doInBackground() throws Exception;
    
    @Override
    protected abstract void process(List<Integer> val);
    
    @Override
    public abstract void done();
    
    public void update(Integer i)
    {
        publish(i);
    }
    
}
