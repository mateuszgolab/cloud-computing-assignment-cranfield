package uk.ac.cranfield.cloudcomputing.assignment.view;

import java.awt.GridLayout;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;

import uk.ac.cranfield.cloudcomputing.assignment.Controller;


public class MainPanel extends JPanel
{
    
    public static final int MATRIX_SIZE_LIMIT = 3000;
    public static final int NODES_NUMBER_LIMIT = 8;
    public static final int DATA_BLOCKS_LIMIT = 4;
    
    private JLabel matrixSizeLabel;
    private JLabel workingNodesLabel;
    private JLabel dataBlocksLabel;
    private JTextField matrixSizeText;
    private JTextField workingNodesText;
    private JTextField dataBlocksText;
    private JButton startMultiplication;
    private JButton startAddition;
    private JButton exitProgram;
    private JLabel localResultsLabel;
    private JLabel distResultsLabel;
    private JLabel validationLabel;
    private JTextField localResultsText;
    private JTextField distResultsText;
    private JTextField validationText;
    
    public MainPanel()
    {
        super(new GridLayout(9, 3, 10, 10));
        matrixSizeLabel = new JLabel("  Matrix size (NxN) N : ");
        matrixSizeText = new JTextField("1000");
        workingNodesLabel = new JLabel("  Number of working nodes : ");
        workingNodesText = new JTextField("8");
        dataBlocksLabel = new JLabel("  Number of data blocks : ");
        dataBlocksText = new JTextField("32");
        localResultsLabel = new JLabel("  Local calculations time : ");
        localResultsText = new JTextField("");
        localResultsText.setEditable(false);
        distResultsLabel = new JLabel("  Distributed calculations time : ");
        distResultsText = new JTextField("");
        distResultsText.setEditable(false);
        validationLabel = new JLabel("  Validation result : ");
        validationText = new JTextField("");
        validationText.setEditable(false);
        
        startAddition = new JButton("start matrix addition");
        startMultiplication = new JButton("start matrix multiplication");
        exitProgram = new JButton("exit program");
        
        
        add(matrixSizeLabel);
        add(matrixSizeText);
        add(workingNodesLabel);
        add(workingNodesText);
        add(dataBlocksLabel);
        add(dataBlocksText);
        add(new JLabel());
        add(startAddition);
        add(new JLabel());
        add(startMultiplication);
        add(new JLabel());
        add(exitProgram);
        add(validationLabel);
        add(validationText);
        add(distResultsLabel);
        add(distResultsText);
        add(localResultsLabel);
        add(localResultsText);
    }
    
    public void addActionListener(Controller listener)
    {
        startAddition.addActionListener(listener);
        startMultiplication.addActionListener(listener);
        exitProgram.addActionListener(listener);
    }
    
    
    public JButton getStartAdditionButton()
    {
        return startAddition;
    }
    
    public JButton getStartMultiplicatioButton()
    {
        return startMultiplication;
    }
    
    public JButton getExitProgramButton()
    {
        return exitProgram;
    }
    
    public int getMatrixSize()
    {
        return Integer.parseInt(matrixSizeText.getText());
    }
    
    public int getNumberOfNodes()
    {
        return Integer.parseInt(workingNodesText.getText());
    }
    
    public int getNumberOfDataBlocks()
    {
        return Integer.parseInt(dataBlocksText.getText());
    }
    
    public void setValidationResult(String text)
    {
        validationText.setText(text);
    }
    
    public void setDistResults(String text)
    {
        distResultsText.setText(text);
    }
    
    public void setLocalResults(String text)
    {
        localResultsText.setText(text);
    }
    
    public void clear()
    {
        localResultsText.setText("");
        distResultsText.setText("");
        validationText.setText("");
    }
    
    public boolean wrongValues()
    {
        
        if (getMatrixSize() > MATRIX_SIZE_LIMIT)
        {
            JOptionPane.showMessageDialog(null, "Matrix size is too big", "Wrong data", JOptionPane.ERROR_MESSAGE);
            return true;
        }
        if (getNumberOfNodes() > NODES_NUMBER_LIMIT)
        {
            JOptionPane
                    .showMessageDialog(null, "Maximum number of nodes is 8", "Wrong data", JOptionPane.ERROR_MESSAGE);
            return true;
        }
        if (getNumberOfDataBlocks() < getNumberOfNodes() * 4)
        {
            JOptionPane.showMessageDialog(null,
                    "Number of data blocks should be at least equal to : number of nodes x 4", "Wrong data",
                    JOptionPane.ERROR_MESSAGE);
            return true;
        }
        if (getMatrixSize() * getMatrixSize() < getNumberOfDataBlocks())
        {
            JOptionPane.showMessageDialog(null, "Number of data blocks is bigger then  number of elements in matrix !",
                    "Wrong data", JOptionPane.ERROR_MESSAGE);
            return false;
        }
        
        return false;
    }
}
