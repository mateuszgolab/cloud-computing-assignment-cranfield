package uk.ac.cranfield.cloudcomputing.assignment.view;

import java.awt.BorderLayout;

import javax.swing.JFrame;

import uk.ac.cranfield.cloudcomputing.assignment.Controller;


public class MainFrame extends JFrame
{
    
    private MainPanel panel;
    private StatusPanel status;
    private Controller controller;
    
    public MainFrame()
    {
        setTitle("Cloud Computing Assignment  author : Mateusz Golab");
        setSize(FrameToolkit.getSize());
        setLocationRelativeTo(null);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        
        panel = new MainPanel();
        status = new StatusPanel();
        
        controller = new Controller(panel, status);
        
        panel.addActionListener(controller);
        
        add(panel, BorderLayout.CENTER);
        add(status, BorderLayout.SOUTH);
        setVisible(true);
        
        pack();
        
        
    }
    
}
