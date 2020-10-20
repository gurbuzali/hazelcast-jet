package com.hazelcast.jet.examples.jobpriorities;

import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryUpdatedListener;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;

import javax.swing.*;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.UUID;

import static java.lang.Long.max;
import static javax.swing.WindowConstants.EXIT_ON_CLOSE;

public class JobPrioritiesGui {
    private static final int WINDOW_X = 100;
    private static final int WINDOW_Y = 100;
    private static final int WINDOW_WIDTH = 1200;
    private static final int WINDOW_HEIGHT = 650;
    private static final int INITIAL_TOP_Y = 5_000_000;

    private final IMap<String, Long> hzMap;
    private UUID entryListenerId;

    public JobPrioritiesGui(IMap<String, Long> hzMap) {
        this.hzMap = hzMap;
        EventQueue.invokeLater(this::startGui);
    }

    private void startGui() {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        CategoryPlot chartFrame = createChartFrame(dataset);
        ValueAxis yAxis = chartFrame.getRangeAxis();

        long[] topY = {INITIAL_TOP_Y};
        EntryUpdatedListener<String, Long> entryUpdatedListener = event -> {
            System.err.println("event = " + event); //todo: remove
            EventQueue.invokeLater(() -> {
                dataset.addValue(event.getValue(), event.getKey(), "");
                topY[0] = max(topY[0], INITIAL_TOP_Y * (1 + event.getValue() / INITIAL_TOP_Y));
                yAxis.setRange(0, topY[0]);
            });
        };
        entryListenerId = hzMap.addEntryListener(entryUpdatedListener, true);
    }

    private CategoryPlot createChartFrame(CategoryDataset dataset) {
        JFreeChart chart = ChartFactory.createBarChart(
                "Job Results", "Job", "Results", dataset,
                PlotOrientation.HORIZONTAL, true, true, false);
        CategoryPlot plot = chart.getCategoryPlot();
        plot.setBackgroundPaint(Color.WHITE);
        plot.setDomainGridlinePaint(Color.DARK_GRAY);
        plot.setRangeGridlinePaint(Color.DARK_GRAY);
        plot.getRenderer().setSeriesPaint(0, Color.BLUE);

        JFrame frame = new JFrame();
        frame.setBackground(Color.WHITE);
        frame.setDefaultCloseOperation(EXIT_ON_CLOSE);
        frame.setTitle("Job Priorities");
        frame.setBounds(WINDOW_X, WINDOW_Y, WINDOW_WIDTH, WINDOW_HEIGHT);
        frame.setLayout(new BorderLayout());
        frame.add(new ChartPanel(chart));
        frame.setVisible(true);
        frame.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent windowEvent) {
                hzMap.removeEntryListener(entryListenerId);
            }
        });
        return plot;
    }
}
