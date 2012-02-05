package uk.ac.cranfield.cloudcomputing.assignment.view;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;


public class StatusPanel extends JPanel
{
    
    private JLabel statusLabel;
    private JScrollPane pane;
    private JTextField statusText;
    
    public StatusPanel()
    {
        statusLabel = new JLabel("Status ");
        statusText = new JTextField(30);
        pane = new JScrollPane(statusText);
        add(statusLabel);
        add(pane);
    }
    
    public void print(String text)
    {
        statusText.setText(text);
        System.out.println(text);
        repaint();
    }
}
