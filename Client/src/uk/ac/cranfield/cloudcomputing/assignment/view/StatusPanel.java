package uk.ac.cranfield.cloudcomputing.assignment.view;

import java.awt.BorderLayout;
import java.awt.Dimension;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTextField;


public class StatusPanel extends JPanel
{
    
    private JLabel statusLabel;
    private JScrollPane pane;
    private JTextField statusText;
    private JProgressBar progressBar;
    
    public StatusPanel()
    {
        super(new BorderLayout());
        progressBar = new JProgressBar(0, 10000);
        progressBar.setPreferredSize(new Dimension(400, 40));
        progressBar.setStringPainted(true);
        
        statusLabel = new JLabel("Status ");
        statusText = new JTextField(50);
        statusText.setEditable(false);
        
        pane = new JScrollPane(statusText);
        
        add(new JLabel("  Calculations progress"), BorderLayout.NORTH);
        
        add(progressBar, BorderLayout.CENTER);
        JPanel stat = new JPanel();
        stat.add(statusLabel);
        stat.add(pane);
        add(stat, BorderLayout.SOUTH);
    }
    
    public void print(String text)
    {
        statusText.setText(text);
        System.out.println(text);
        repaint();
    }
    
    public void updateProgress(int val)
    {
        progressBar.setValue(val);
        repaint();
    }
    
    public void reset()
    {
        progressBar.setValue(0);
        statusText.setText("");
    }
}
